from ep.eia_data_aggregator import EIADataGetter, EIADataAggregator
import pandas as pd

if __name__ == '__main__':
    # d = EIADataGetter()
    # d.update_local_eia_data()
    # output=[test_list[i:i + n] for i in range(0, len(test_list), n)]
    e = EIADataGetter()
    EIADataAggregator.validate_eia_report_format()

    # df = pd.DataFrame(d.items(), columns=['ref_period', file_name])
    #
    # df.index = df['ref_period']
    # df.drop('ref_period', axis=1, inplace=True)