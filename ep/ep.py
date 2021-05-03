import requests
import pandas as pd
import os
import json
from typing import Dict, List

from app_config.App import App


class EIADataGetter:

    def __init__(self, config_loc: str = "../config/eia_getter_config.json"):
        with open(config_loc, "r") as ec:
            self.config = json.load(ec)

        self.eia_site = self.config.get("eia_root")
        self.local_data = "downloads"

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
            f for f in files_available_for_download if f.split("/")[0] not in files_downloaded
        ]

        for p in paths_to_download:
            r = requests.get(self.eia_site + p)
            with open('local_' + p, 'wb') as f:
                f.write(r.content)


class EIADataValidation:
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
                lambda m: m.lower() in fn or m in fn, EIADataProcessor.months_to_num.keys()
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

        mY = fn.split("_")[0]
        return "20" \
               + mY[3:] \
               + "-" \
               + (
                   str(EIADataProcessor.months_to_num.get(mY.lower()[:3]))
                   if EIADataProcessor.months_to_num.get(mY.lower()[:3]) > 9
                   else "0" + str(EIADataProcessor.months_to_num.get(mY.lower()[:3]))
               )


class EIADataProcessor:

    def __init__(self, config_loc: str = "../config/eia_getter_config.json"):
        with open(config_loc, "r") as ec:
            self.config = json.load(ec)

    keys = App.config("eia_keys")
    months = App.config("months")
    months_to_num = dict(
        zip(months, range(1, 13))
    )

    @staticmethod
    def shift_eia_date_my_months(eia_date: str, months: int) -> str:
        """

        :param eia_date: assumes format 2021-1, 2021-2
        :return:
        """
        eia_year, eia_month = [int(d) for d in eia_date.split("-")]
        if eia_month > 12 or eia_month < 0:
            raise ValueError(f"EIA Report Date {eia_date} should be in YYYY-MM format")
        result_year = (eia_year * 12 + (eia_month - 1) + months) // 12;
        result_month = ((eia_year * 12 + (eia_month - 1) + months) % 12) + 1
        return str(result_year) + "-" + (str(result_month) if result_month > 9 else "0" + str(result_month))

    @staticmethod
    def validate_eia_report_format(dir='C:/Users/dvese/PycharmProjects/testBC/venv/Scripts/local_archives/'):
        reports = [x for x in os.listdir(dir) if '#' not in x]  # to ignore lock files
        aggregate_tables = {}
        for k in EIADataProcessor.keys:
            aggregate_tables.update({k: pd.DataFrame()})

        for f in reports:
            print(f"Processing {f}...")
            ts_name = EIADataValidation.check_name_of_file_and_map_to_ts_name(f)

            xl = pd.ExcelFile(dir + f)
            df = xl.parse(App.config("data_excel_tab"), header=None)
            positions_of_timeseries = EIADataValidation.check_key_is_present(
                df, EIADataProcessor.keys
            )
            # note if is assumed all timeseries stRT with Jan
            start_of_ts = EIADataValidation.find_start_of_timeseries(
                df, App.config("time_series_start_marker")
            )

            timeseries_series = EIADataProcessor.extract_times_series_data_from_df(
                start_of_ts, positions_of_timeseries, df, ts_name
            )
            for k in aggregate_tables:
                _ = pd.concat(
                    [aggregate_tables.get(k), timeseries_series.get(k)], axis=1
                )
                aggregate_tables.update({k: _})

        for k in aggregate_tables:
            print(f"Saving {k}")
            aggregate_tables.get(k).sort_index(inplace=True, axis=0)
            aggregate_tables.get(k).sort_index(inplace=True, axis=1)
            aggregate_tables.get(k).to_csv("../aggregates/{}.csv".format(k))

    @staticmethod
    def extract_times_series_data_from_df(
            start_of_ts, positions_of_timeseries, df, ts_name, return_pd_series=True):
        dates = []
        timeseries = {}
        for k in EIADataProcessor.keys:
            timeseries.update({k: []})

        for ind in range(start_of_ts[0], df.shape[1]):
            date_m = df[ind][start_of_ts[1]]
            if not pd.isna(df[ind][start_of_ts[1] - 1]):
                date_y = df[ind][start_of_ts[1] - 1]
            dates.append(
                str(date_y) + "-" + (
                    str(EIADataProcessor.months_to_num.get(date_m.lower()))
                    if EIADataProcessor.months_to_num.get(date_m.lower()) > 9
                    else "0" + str(EIADataProcessor.months_to_num.get(date_m.lower()))
                )
            )
            for k in EIADataProcessor.keys:
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
    EIADataProcessor.validate_eia_report_format()

