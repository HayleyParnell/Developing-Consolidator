{
    "level1": {
        "columns": [
            "Social Security Number",
            "First Name or Initial",
            "Middle Name or Initial",
            "Last Name",
            "Suffix",
            "Date of Birth"
        ],
        "aggregation_functions": {
            "Address": "join_unique_address",
            "City": "join_unique_address",
            "State": "join_unique_address",
            "ZIP Code": "join_unique_address",
            "Country (if not USA)": "join_unique_address",
            "Financial Account Number": "join_unique",
            "Driver's License Number / State ID Number": "join_unique",
            "PII": "join_unique",
            "Associated Docs (People)": "join_unique", 
            "People Tracker Control Number": "join_unique"
        }
    },
    "level2": {
        "columns": [
            "Social Security Number",
            "First Name or Initial",
            "Middle Name or Initial",
            "Last Name",
            "Suffix",
            "Address",
            "City",
            "State",
            "ZIP Code",
            "Country (if not USA)",
            "Date of Birth",
            "Financial Account Number",
            "Driver's License Number / State ID Number",
            "PII",
            "Associated Docs (People)",
            "People Tracker Control Number"
        ],
        "aggregation_functions": {
            "Date of Birth": "join_unique"
        }
    }
}
