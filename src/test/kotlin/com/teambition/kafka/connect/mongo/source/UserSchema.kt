package com.teambition.kafka.connect.mongo.source

/**
 * @author Xu Jingxin
 */
object UserSchema {
    val record = """
{
  "type": "record",
  "name": "mongo_schema_teambition_users",
  "fields": [
    {
      "name": "__op",
      "type": "string",
      "default": null
    },
    {
      "name": "__pkey",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "sqlType": "VARCHAR(100)"
          }
        }
      ],
      "default": null
    },
    {
      "name": "name",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "sqlType": "VARCHAR"
          }
        }
      ],
      "default": null
    },
    {
      "name": "updated",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "sqlType": "TIMESTAMP"
          }
        }
      ],
      "default": null
    }
  ],
  "connect.parameters": {
    "table": "base_users"
  },
  "connect.name": "mongo_schema_teambition_users"
}
    """.trimIndent()
}
