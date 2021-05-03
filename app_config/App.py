class App:

    __conf = {
        "data_store": "./eia_local_store",
        "eia_root": "https://www.eia.gov/outlooks/steo/",
        "eia_index_page": "outlook.php",
        "eia_archive_dir": "archive",
        "eia_keys": [
            "WTIPUUS",
            "BREPUUS",
            "MGWHUUS",
            "DSWHUUS",
            "D2WHUUS",
            "MGRARUS",
            "MGEIAUS",
            "DSRTUUS",
            "D2RCAUS",
            "NGHHUUS",
            "NGRCUUS",
            "ESRCUUS"
        ],

        "months": ["jan", "feb", "mar",
                   "apr", "may", "jun",
                   "jul", "aug", "sep",
                   "oct", "nov", "dec"
                   ],

        "data_excel_tab": "2tab",
        "time_series_start_marker": "Jan",
    }

    @staticmethod
    def config(name):
        return App.__conf[name]