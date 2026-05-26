# =========================================================
# DATASET REGISTRY
# =========================================================

DATASETS = {

    "client_reporting_date_dataset": {

        "description":
        "Daily advertiser performance metrics",

        "use_cases": [

            "spend trends",
            "roas trends",
            "ctr analysis",
            "conversion analysis",
            "device performance",
            "browser performance",
            "creative performance",
            "daily reporting"

        ],

        "important_columns": [

            "date",
            "advertiser",
            "spend",
            "revenue",
            "impressions",
            "clicks",
            "ctr",
            "roas",
            "device",
            "browser"

        ]

    },

    "client_reporting_geo_dataset": {

        "description":
        "Geographic advertiser metrics",

        "use_cases": [

            "state analysis",
            "country analysis",
            "dma analysis",
            "regional analysis",
            "geo performance"

        ],

        "important_columns": [

            "date",
            "advertiser",
            "state",
            "region",
            "country",
            "dma",
            "spend",
            "revenue",
            "roas"

        ]

    },

    "client_reporting_network_dataset_myreports": {

        "description":
        "Publisher and network performance metrics",

        "use_cases": [

            "publisher analysis",
            "site analysis",
            "network performance",
            "supply vendor analysis"

        ],

        "important_columns": [

            "date",
            "advertiser",
            "publisher",
            "site",
            "network",
            "spend",
            "revenue",
            "roas"

        ]

    },

    "client_reporting_hour_dataset": {

        "description":
        "Hourly and daypart advertiser metrics",

        "use_cases": [

            "hour analysis",
            "daypart analysis",
            "hourly trends",
            "best performing hours"

        ],

        "important_columns": [

            "date",
            "hour",
            "advertiser",
            "spend",
            "revenue",
            "roas"

        ]

    }

}
