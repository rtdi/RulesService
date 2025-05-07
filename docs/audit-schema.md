This is the `__audit` field definition within the Avro schema.

```
    {
      "name": "__audit",
      "__internal": true,
      "__originalname": "__audit",
      "default": null,
      "doc": "If data is transformed this information is recorded here",
      "type": [
        "null",
        {
          "__originalname": "__audit",
          "doc": "If data is transformed this information is recorded here",
          "fields": [
            {
              "__originalname": "__transformresult",
              "doc": "Is the record PASS, FAILED or WARN?",
              "name": "__transformresult",
              "type": {
                "length": 4,
                "logicalType": "VARCHAR",
                "type": "string"
              }
            },
            {
              "__originalname": "__details",
              "default": null,
              "doc": "Details of all transformations",
              "name": "__details",
              "type": [
                "null",
                {
                  "items": {
                    "__originalname": "__audit_details",
                    "doc": "Details of all transformations",
                    "fields": [
                      {
                        "__originalname": "__transformationname",
                        "doc": "A name identifying the applied transformation",
                        "name": "__transformationname",
                        "type": {
                          "length": 1024,
                          "logicalType": "NVARCHAR",
                          "type": "string"
                        }
                      },
                      {
                        "__originalname": "__transformresult",
                        "doc": "Is the record PASS, FAIL or WARN?",
                        "name": "__transformresult",
                        "type": {
                          "length": 4,
                          "logicalType": "VARCHAR",
                          "type": "string"
                        }
                      },
                      {
                        "__originalname": "__transformresult_text",
                        "doc": "Transforms can optionally describe what they did",
                        "name": "__transformresult_text",
                        "type": [
                          "null",
                          {
                            "length": 1024,
                            "logicalType": "NVARCHAR",
                            "type": "string"
                          }
                        ]
                      },
                      {
                        "__originalname": "__transformresult_quality",
                        "doc": "Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)",
                        "name": "__transformresult_quality",
                        "type": [
                          "null",
                          {
                            "logicalType": "BYTE",
                            "type": "int"
                          }
                        ]
                      }
                    ],
                    "name": "__audit_details",
                    "type": "record"
                  },
                  "type": "array"
                }
              ]
            }
          ],
          "name": "__audit",
          "namespace": "",
          "type": "record"
        }
      ]
    }
```