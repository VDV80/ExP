import requests
import pandas as pd
import os
import json
from typing import Dict

from app_config.App import App


class EIADataGetter:

    def __init__(self, config_loc: str = "../config/eia_getter_config.json"):
        with open(config_loc, "r") as ec:
            self.config = json.load(ec)

        self.eia_site = self.config.get("eia_root")
        self.local_data = self.config.get("data_store")

    def get_list_of_available_forecast_files(self):
        files_to_download = []
        _ = requests.get(self.eia_site + App.config("eia_index_page"))
        for l in _.iter_lines():
            if "xlsx" in str(l):
                files_to_download += [x for x in str(l).split("\"") if "xlsx" in x]

        files_to_download = [
            x for x in files_to_download if self.config.get("eia_archive_dir") in x
        ]

        return files_to_download

    def get_forecasts_available_locally(self):
        from os import listdir
        local_forecast_files = listdir(self.local_data)
        return local_forecast_files

    def update_local_eia_data(self):
        files_available_for_download = self.get_list_of_available_forecast_files()
        files_downloaded = self.get_forecasts_available_locally()
        paths_to_download = [
            f for f in files_available_for_download if f.split("/")[1] not in files_downloaded
        ]

        for p in paths_to_download:
            r = requests.get(self.eia_site + p)
            file_loc = self.config.get("data_store") + p.split("/")[1]
            with open(file_loc, 'wb') as f:
                print(f"Saving {file_loc}")
                f.write(r.content)


class EIADataValidator:
    """
    Just a collection of rules to check before processing the files
    """

    @staticmethod
    def find_start_of_timeseries(df, identifier):
        located = None
        for i in range(15):
            for j in range(15):
                if df[i][j] == identifier:
                    return i, j
        if not located:
            raise ValueError(f"Failed to locate start of timeseries in {f} "
                             f"using identifier {identifier}. Please check contents"
                             f"of the input file {f}")

    @staticmethod
    def check_key_is_present(df, keys):
        keys_location: Dict[str, int] = {}
        for k in keys:
            try:
                keys_location.update(
                    {k: list(df[0]).index(k)}
                )
            except Exception as e:
                raise Exception(f"Failed to locate timeseries key: {k} in file {f}."
                                f"Please check contents of the input file {f}")

        return keys_location

    @staticmethod
    def check_name_of_file_and_map_to_ts_name(fn):
        check1 = any(
            map(
                lambda m: m.lower() in fn or m in fn, EIADataAggregator.months_to_num.keys()
            )
        )
        check2 = any(
            map(
                lambda year: str(year) in fn, range(10, 99)
            )
        )
        if not all([check1, check2]):
            if not check1:
                raise ValueError(f"Could not identify month from file name {fn} ")
            else:
                raise ValueError(f"Could not identify year from file name {fn} ")

        mmmyy = fn.split("_")[0]
        return "20" \
               + mmmyy[3:] \
               + "-" \
               + (
                   str(EIADataAggregator.months_to_num.get(mmmyy.lower()[:3]))
                   if EIADataAggregator.months_to_num.get(mmmyy.lower()[:3]) > 9
                   else "0" + str(EIADataAggregator.months_to_num.get(mmmyy.lower()[:3]))
               )


class EIADataAggregator:

    def __init__(self, config_loc: str = "../config/eia_getter_config.json"):
        with open(config_loc, "r") as ec:
            self.config = json.load(ec)

    keys = App.config("eia_keys")
    months = App.config("months")
    months_to_num = dict(
        zip(months, range(1, 13))
    )

    @staticmethod
    def validate_eia_report_format(dir=App.config("data_store")):
        reports = [x for x in os.listdir(dir) if '#' not in x]  # to ignore lock files
        aggregate_tables = {}
        for k in EIADataAggregator.keys:
            aggregate_tables.update({k: pd.DataFrame()})

        for f in reports:
            print(f"Validating {f}...")
            ts_name = EIADataValidator.check_name_of_file_and_map_to_ts_name(f)

            xl = pd.ExcelFile(dir + f)
            df = xl.parse(App.config("data_excel_tab"), header=None)
            positions_of_timeseries = EIADataValidator.check_key_is_present(
                df, EIADataAggregator.keys
            )
            # note if is assumed all timeseries stRT with Jan
            start_of_ts = EIADataValidator.find_start_of_timeseries(
                df, App.config("time_series_start_marker")
            )

            timeseries_series = EIADataAggregator.extract_times_series_data_from_df(
                start_of_ts, positions_of_timeseries, df, ts_name
            )
            for k in aggregate_tables:
                _ = pd.concat(
                    [aggregate_tables.get(k), timeseries_series.get(k)], axis=1
                )
                aggregate_tables.update({k: _})

        for k in aggregate_tables:
            print(f"Saving {k} to " + "../aggregates/{}.csv".format(k))
            aggregate_tables.get(k).sort_index(inplace=True, axis=0)
            aggregate_tables.get(k).sort_index(inplace=True, axis=1)
            aggregate_tables.get(k).to_csv("../aggregates/{}.csv".format(k))

    @staticmethod
    def extract_times_series_data_from_df(
            start_of_ts, positions_of_timeseries, df, ts_name, return_pd_series=True):
        dates = []
        timeseries = {}
        for k in EIADataAggregator.keys:
            timeseries.update({k: []})

        for ind in range(start_of_ts[0], df.shape[1]):
            date_m = df[ind][start_of_ts[1]]
            if not pd.isna(df[ind][start_of_ts[1] - 1]):
                date_y = df[ind][start_of_ts[1] - 1]
            dates.append(
                str(date_y) + "-" + (
                    str(EIADataAggregator.months_to_num.get(date_m.lower()))
                    if EIADataAggregator.months_to_num.get(date_m.lower()) > 9
                    else "0" + str(EIADataAggregator.months_to_num.get(date_m.lower()))
                )
            )
            for k in EIADataAggregator.keys:
                timeseries.get(k).append(df[ind][positions_of_timeseries.get(k)])

        if return_pd_series:
            timeseries_series = {}
            for k in timeseries:
                timeseries_series.update(
                    {k: pd.Series(timeseries.get(k), index=dates, name=ts_name)}
                )
            return timeseries_series
        else:
            return dates, timeseries


if __name__ == '__main__':
    e = EIADataGetter()
    e.update_local_eia_data()
    EIADataAggregator.validate_eia_report_format()
