package com.teambition.kafka.connect.mongo.tools

import com.github.kittinunf.fuel.Fuel
import org.json.JSONArray
import org.json.JSONObject

/**
 * @author Xu Jingxin
 */
object Application {
    private const val help = """
    usage:
        kafka-connect-mongo <command> [options]

    commands:
        importdata [config file path]           import data from mongodb to kafka
        healthcheck                             health check for all the tasks of connectors
"""

    fun help() = println(help)

    fun healthcheck() {
        val port = System.getenv("CONNECT_REST_PORT")
        val host = System.getenv("CONNECT_REST_ADVERTISED_HOST_NAME")
        val url = "http://127.0.0.1:$port/connectors"
        val workerId = "$host:$port"

        val connectors = Fuel.get(url).responseString().let { (_, _, result) ->
            val (str, e) = result
            if (e != null) throw e
            JSONArray(str).toList()
        }

        if (connectors.isEmpty()) {
            return println("connector is empty.")
        }

        connectors.forEach { it ->
            println("check connector $it")

            Fuel.get("$url/$it/status")
                .responseString()
                .let { (_, _, result) ->
                    val (str, e) = result
                    if (e != null) throw e
                    JSONObject(str)["tasks"] as JSONArray
                }
                .forEach {
                    val task = (it as JSONObject).toMap()
                    if (task["state"] == "FAILED" && task["worker_id"] == workerId)
                        throw Exception("task ${task["id"]} failed")
                }
        }

        println("check finish, without errors.")
    }
}

fun main(args: Array<String>) {
    when (args.firstOrNull()) {
        "importdata" -> ImportData.run(args.slice(1 until args.size).toTypedArray())
        "healthcheck" -> Application.healthcheck()
        else -> Application.help()
    }
}
