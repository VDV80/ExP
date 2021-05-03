import os
import pandas as pd


class ForecastEvaluator:

    def __init__(self):
        self.forecast_tables = {}
        for f in os.listdir("../aggregates"):
            self.forecast_tables.update(
                {f.split(".")[0]: pd.read_csv("../aggregates/" + f, index_col=0)}
            )
            print(f"Loaded {f}...")

    def aggregate_forecast_vs_actual(self):
        res = {}
        for k in self.forecast_tables:
            t = self.forecast_tables.get(k)
            res_inner = []
            for c in t.columns:
                forecast_perios = 12
                if ForecastEvaluator.shift_eia_date_my_months(c, 1 + forecast_perios) in t.columns:
                    res_inner.append(
                        (
                            self.get_nth_month_forecast_from_vintage(c, forecast_perios, t),
                            self.get_actual_for_forecasted_period(
                                ForecastEvaluator.shift_eia_date_my_months(c, forecast_perios - 1), t
                            )
                        )
                    )

            o = map(
                lambda x: x[0] - x[1], res_inner
            )
            from matplotlib import pyplot as plt
            plt.hist(list(o), bins=20)
            plt.show()
            res.update({k: res_inner})

    def get_nth_month_forecast_from_vintage(self, vintage, n_month, forecast_table):
        return \
            forecast_table[vintage][ForecastEvaluator.shift_eia_date_my_months(vintage, -1 + n_month)]

    def get_actual_for_forecasted_period(self, forecasted_period, forecast_table):
        return \
            forecast_table[ForecastEvaluator.shift_eia_date_my_months(forecasted_period, 1)][forecasted_period]

    @staticmethod
    def shift_eia_date_my_months(eia_date: str, months: int) -> str:
        """

        :param months:
        :param eia_date: assumes format 2021-01, 2021-02
        :return:
        """
        eia_year, eia_month = [int(d) for d in eia_date.split("-")]
        if eia_month > 12 or eia_month < 0:
            raise ValueError(f"EIA Report Date {eia_date} should be in YYYY-MM format")
        result_year = (eia_year * 12 + (eia_month - 1) + months) // 12;
        result_month = ((eia_year * 12 + (eia_month - 1) + months) % 12) + 1
        return str(result_year) + "-" + (str(result_month) if result_month > 9 else "0" + str(result_month))


if __name__ == '__main__':
    fe = ForecastEvaluator()
    fe.aggregate_forecast_vs_actual()