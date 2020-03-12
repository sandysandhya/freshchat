package com.saras.plugins

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.TypeAdapter
import com.google.gson.annotations.SerializedName
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter
import com.saras.pipelines.*
import com.saras.pipelines.sources.SourceProcess
import com.saras.pipelines.sources.SourceProcessFactory
import io.reactivex.Emitter
import org.apache.deltaspike.core.api.config.ConfigResolver
import org.pf4j.Extension
import org.slf4j.LoggerFactory
import retrofit2.Call
import retrofit2.Retrofit
import retrofit2.converter.scalars.ScalarsConverterFactory
import retrofit2.http.*
import java.lang.reflect.Modifier
import java.text.SimpleDateFormat
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import java.util.stream.StreamSupport
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.functions
import kotlin.reflect.jvm.javaField
import kotlin.streams.toList

class FreshchatSourceProcess(override val source: Source) : SourceProcess {
    override fun refreshToken(): Pair<Boolean, SourceToken> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun process(sourceLogs: Map<String, SourceLog>, emitter: Emitter<Message>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun process(sourceLog: SourceLog, table: Table, emitter: Emitter<Message>,extras: Extras) {
        logger.info(
            "Stampedio source process started for SourceId = ${source.sourceId}, UserId = ${source.userId}," +
                    " SourceName = ${source.name}"
        )
        val currentDateTime = LocalDateTime.now()
        try {
            var accessToken=source.settings.token.token
            val columns = table.columns.filter { it.selected }
            val log = sourceLog
            val lastRunRecordDate = log.lastRecord
            val historyYears = if (source.history == "") {
                1
            } else {
                source.history.toLong()
            }
            val currentLastRunRecord = currentDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
            val lastHistoryDateTime =
                currentDateTime.minusYears(historyYears).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
            val lastRunDate = if (lastRunRecordDate > 0) {
                lastRunRecordDate
            } else {
                lastHistoryDateTime
            }
            log.currentLastRecord = currentLastRunRecord
            val tableName = table.name.substringAfter("_")
            when (tableName) {
//                "Getproductlist" -> {
//                    var reportRecords = productlist(accessToken,columns, lastRunDate,currentLastRunRecord, tableName, emitter, log)
//                    emitData(reportRecords, columns, tableName, emitter, lastRunDate, log)
//                }
                "listappointments" -> {
                    var reportRecords = listappointments(accessToken,tableName)
                    emitData(reportRecords, columns, tableName, emitter, lastRunDate, log)
                }

            }
            emitter.onComplete()
        } catch (e: Exception) {

            emitter.onError(e)
        }
    }

    private val batchId = AtomicLong()
    private val timestamp = AtomicLong()

    override fun userId() = source.userId

    override fun start(): Boolean {
        timestamp.set(Instant.now().toEpochMilli())
        logger.info("Freshchat source started for ${source.sourceId}")
        return true
    }



    private fun exchangeToken(accessToken:String,tableName: String):JsonNode {

        val resp = freshchatAPI.exchangeToken("grant_type","client_id","client_secret","refresh_token").execute()
        if (resp.isSuccessful) {
            val r4 = resp.body() ?: throw RuntimeException("freshchat code error")
            val j4 = mapper.readTree(r4)
            j4["error"]?.let {
                throw RuntimeException(it.toString())
            }
            return  j4
        }else{
            when (resp.code()) {
                401 -> throw RuntimeException("Authentication Failed. " + resp.errorBody()?.string())
                402 -> throw RuntimeException("Payment Required. " + resp.errorBody()?.string())
                429 -> {
                    val responseHeader = resp.headers()
                    val timeToReset = responseHeader["X-RateLimit-Reset"]?.toLong() ?: 0L
                    val sleepOffset = Instant.now().epochSecond - timeToReset
                    TimeUnit.SECONDS.sleep(sleepOffset)
                }
                else -> {
                    FreshchatSourceProcess.logger.error("Problem occurred while fetching Hubspot from API")
                }
            }
            throw  RuntimeException("Error at $tableName Table" + resp.errorBody()?.string())
        }

    }


    private fun listappointments(accessToken:String,tableName: String):JsonNode {

        val resp = freshchatAPI.listappointments("Authorization","since","until","limit").execute()
        if (resp.isSuccessful) {
            val r4 = resp.body() ?: throw RuntimeException("Freshchat code error")
            val j4 = mapper.readTree(r4)
            j4["error"]?.let {
                throw RuntimeException(it.toString())
            }
            return  j4
        }else{
            when (resp.code()) {
                401 -> throw RuntimeException("Authentication Failed. " + resp.errorBody()?.string())
                402 -> throw RuntimeException("Payment Required. " + resp.errorBody()?.string())
                429 -> {
                    val responseHeader = resp.headers()
                    val timeToReset = responseHeader["X-RateLimit-Reset"]?.toLong() ?: 0L
                    val sleepOffset = Instant.now().epochSecond - timeToReset
                    TimeUnit.SECONDS.sleep(sleepOffset)
                }
                else -> {
                    FreshchatSourceProcess.logger.error("Problem occurred while fetching Freshchat from API")
                }
            }
            throw  RuntimeException("Error at $tableName Table" + resp.errorBody()?.string())
        }

    }






    private fun emitData(data: JsonNode, fieldList: List<Column>, tableName: String, emitter: Emitter<Message>, lastRunDate: Long, log: SourceLog?) {
        val lastRunDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastRunDate), ZoneId.systemDefault()).toLocalDate()
        var createdDate = lastRunDate
        var maxCreatedDate = createdDate

        data.map { element ->
            val builder = getMessageBuilder(fieldList, tableName, element)
            builder.withBatchId(batchId.incrementAndGet())
            builder.build(fieldList)

        }.forEach {
            println("$it")
            emitter.onNext(it)
        }
        log?.currentLastRecord = maxCreatedDate.atTime(0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli()
    }

    private fun EmitData(data: JsonNode, fieldList: List<Column>, tableName: String, emitter: Emitter<Message>, lastRunDate: Long, log: SourceLog?) {
        val lastRunDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastRunDate), ZoneId.systemDefault()).toLocalDate()
        var createdDate = lastRunDate
        var maxCreatedDate = createdDate

        data.map { element ->

            //            var x = element.get("review")
            val builder = getMessageBuilder(fieldList, tableName, element)
            builder.withBatchId(batchId.incrementAndGet())
            builder.build(fieldList)

        }.forEach {
            println("$it")
            emitter.onNext(it)
        }
        log?.currentLastRecord = maxCreatedDate.atTime(0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli()
    }

    private fun getMessageBuilder(fieldList: List<Column>, tableName: String, element: JsonNode): Message.Builder {
        val builder = Message.newBuilder()
        builder.withCreated(Instant.now().toEpochMilli())
        builder.withTable(tableName)
        builder.withUserId(userId())
//        builder.withBatchId(batchId.incrementAndGet())
        val mappedList = fieldMapper(element)
        for (field in fieldList) {
            val value = element[field.name]

            value?.let { v ->
                when (field.type) {
                    ColumnType.STRING -> {
                        if (v.nodeType == JsonNodeType.ARRAY)
                            builder.withField(field.name, v.joinToString())
                        else
                            builder.withField(field.name, v.asText())
                    }
                    ColumnType.FLOAT -> builder.withField(field.name, v.asDouble())
                    ColumnType.INTEGER -> builder.withField(field.name, v.asInt())
                    ColumnType.NUMERIC -> builder.withField(field.name, v.asDouble())
                    ColumnType.BOOLEAN -> builder.withField(field.name, v.asBoolean())
                    ColumnType.RECORD -> {
                        when {
                            v.nodeType == JsonNodeType.ARRAY -> {
                                val childArray = arrayListOf<JsonObject>()
                                v.forEach {
                                    val childNode = JsonObject()
                                    val cNode = jsonMessageGenerator(field.fields, it, childNode)
                                    childArray.add(cNode)
                                }
                                builder.withField(field.name, childArray)
                            }
                            v.nodeType == JsonNodeType.OBJECT -> {
                                val childNode = JsonObject()
                                val cNode = jsonMessageGenerator(field.fields, v, childNode)
                                builder.withField(field.name, arrayOf(cNode))
                            }
                            else -> builder.withField(field.name, v.joinToString())
                        }
                    }

                    else -> {
                    }
                }
            }
        }
        return builder
    }

    private fun jsonMessageGenerator(fieldList: List<Column>, element: JsonNode, childNode: JsonObject): JsonObject {

        for (field in fieldList) {
            val value = element[field.name]

            value?.let { v ->
                when (field.type) {
                    ColumnType.STRING -> {
                        if (v.nodeType == JsonNodeType.ARRAY)
                            childNode.addProperty(field.name, v.joinToString())
                        else
                            childNode.addProperty(field.name, v.asText())
                    }
                    ColumnType.FLOAT -> childNode.addProperty(field.name, v.asDouble())
                    ColumnType.INTEGER -> childNode.addProperty(field.name, v.asInt())
                    ColumnType.NUMERIC -> childNode.addProperty(field.name, v.asDouble())
                    ColumnType.BOOLEAN -> childNode.addProperty(field.name, v.asBoolean())
                    ColumnType.RECORD -> {
                        when {
                            v.nodeType == JsonNodeType.ARRAY -> {
                                val arrayNode = JsonArray()
                                v.forEach {
                                    val childNode2 = JsonObject()
                                    val temp = jsonMessageGenerator(field.fields, it, childNode2)
                                    arrayNode.add(temp)
                                }
                                childNode.add(field.name, arrayNode)
                            }
                            v.nodeType == JsonNodeType.OBJECT -> {
                                val childNode2 = JsonObject()
                                val arrayNode = JsonArray()
                                val temp = jsonMessageGenerator(field.fields, v, childNode2)
                                arrayNode.add(temp)
                                childNode.add(field.name, arrayNode)
                            }
                            else -> childNode.addProperty(field.name, v.joinToString())
                        }
                    }

                    else -> {
                    }
                }

            }
        }
        return childNode
    }

    private fun fieldMapper(element: JsonNode): MutableList<Map<String, JsonNode>> {
        val mappedList = mutableListOf<Map<String, JsonNode>>()
        element.fields().forEach { e ->
            val fieldMap = mutableMapOf<String, JsonNode>()
            fieldMap.put(e.key.replace("$", "").replace(" ", "").replace(":", ""), e.value)
            mappedList.add(fieldMap)
        }
        return mappedList
    }

    override fun totalRecords() = batchId.get()


    override fun close() {
        FreshchatSourceProcess.logger.info("Freshchat source closed for ${source.sourceId}")
    }

    companion object {
        private val logger = LoggerFactory.getLogger(FreshchatSourceProcess::class.java)
        private val mapper = ObjectMapper()
        private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    }
}

interface FreshchatAPI {
    @POST("oauth/v1/token")
    @FormUrlEncoded
    fun exchangeToken(
        @Field("grant_type") grantType: String = "refresh_token",
        @Field("client_id") clientId: String,
        @Field("client_secret") clientSecret: String,
        @Field("refresh_token") refreshToken: String
    ): Call<String>
    @GET("/appointments")
    fun listappointments(
        @Header("Authorization") Authorization: String,
        @Query("since") since: String,
        @Query("until") until: String,
        @Query("limit") limit: String

    ): Call<String>

}




private val Uri = ConfigResolver.getPropertyValue("freshchat.url")
private val freshchatAPI = Retrofit.Builder().baseUrl("$Uri/").addConverterFactory(ScalarsConverterFactory.create()).build().create(FreshchatAPI::class.java)


private fun checkDatePass(date1: LocalDateTime, date2: LocalDateTime): Boolean {
    return date1.toLocalDate() < date2.toLocalDate()
}


@Extension
class FreshchatSourceProcessFactory : SourceProcessFactory {
    override fun type() = "Freshchat"

    override fun instance(source: Source): SourceProcess = FreshchatSourceProcess(source)
}
