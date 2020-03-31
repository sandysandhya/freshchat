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
    var users= mutableListOf<String>()

    override fun process(sourceLogs: Map<String, SourceLog>, emitter: Emitter<Message>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun process(sourceLog: SourceLog, table: Table, emitter: Emitter<Message>,extras: Extras) {
        logger.info(
            "Freshchat source process started for SourceId = ${source.sourceId}, UserId = ${source.userId}," +
                    " SourceName = ${source.name}"
        )
        val currentDateTime = LocalDateTime.now()
        try {
            val apiToken=source.settings.token.token
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
            val tableName = table.name
            setusers(apiToken)
            when (tableName) {
             "list_groups"-> {
                 var reportRecords = groups(apiToken, tableName)
                 for(row in reportRecords)
                 emitData(row, columns, tableName, emitter,lastRunDate, log)
             }
             "list_channels"  -> {
                 var reportRecords = channels(apiToken,tableName)
                 for(row in reportRecords)
                 emitData(row, columns, tableName, emitter, lastRunDate, log)
             }
                "list_agents" -> {
                    var reportRecords = agent(apiToken,tableName)
                    for(row in reportRecords)
                        emitData(row, columns, tableName, emitter, lastRunDate, log)
                }
                "list_users" -> {
                    var reportRecords =user(apiToken,tableName)
                    Emitdata(reportRecords, columns, tableName, emitter, lastRunDate, log)
                }

//                "Conversation" -> {
//                    var reportRecords = conversation(apiToken,tableName)
//                    emitData(reportRecords, columns, tableName, emitter, lastRunDate, log)
//                }




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


    private fun setusers(apiToken:String) {
        val resp1 = freshchatAPI.getuserids("Bearer $apiToken").execute()
        if (resp1.isSuccessful) {
            val r4 = resp1.body() ?: throw RuntimeException("Freshchat code error")
            val j4 = mapper.readTree(r4).get("agents")
            j4.map { element ->
                var x= j4.get(0)
                users.add(x.get("id").asText())
            }
        }

    }

    private fun groups(apiToken:String,tableName: String):MutableList<JsonNode> {
        var finaljson = mutableListOf<JsonNode>()
        var flag=true
        var page = 1
        var items_per_page=1
        val resp = freshchatAPI.group("Bearer $apiToken" ,page,items_per_page).execute()
        if (resp.isSuccessful) {
            val r4 = resp.body() ?: throw RuntimeException("freshchat code error")
            val j4 = mapper.readTree(r4)
            finaljson.add(j4["groups"])
            if(j4.get("links").has("next_page"))
                page++
            else
                flag=false
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
        while(flag){
            val resp = freshchatAPI.group("Bearer $apiToken",page,items_per_page).execute()
            if (resp.isSuccessful) {
                val r4 = resp.body() ?: throw RuntimeException("freshchat error")
                val j3 =mapper.readTree(r4)
                j3["error"]?.let {
                    throw RuntimeException(it.toString())
                }
                finaljson.add(j3["groups"])
                if(j3.get("links").has("next_page"))
                    page++
                else
                    flag=false
            }

        }
        return finaljson
    }

    private fun agent(apiToken:String,tableName: String):MutableList<JsonNode> {
        var finaljson = mutableListOf<JsonNode>()
        var flag=true
        var page = 1
        var items_per_page=1
        val resp = freshchatAPI.agent("Bearer $apiToken",page,items_per_page).execute()
        if (resp.isSuccessful) {
            val r4 = resp.body() ?: throw RuntimeException("freshchat code error")
            val j4 = mapper.readTree(r4)
            j4["error"]?.let {
                throw RuntimeException(it.toString())
            }
            finaljson.add(j4["agents"])
            if(j4.get("links").has("next_page"))
                page++
            else
                flag=false

        }
        else{
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
                    FreshchatSourceProcess.logger.error("Problem occurred while fetching Freshchatfrom API")
                }
            }
            throw  RuntimeException("Error at $tableName Table" + resp.errorBody()?.string())
        }
        while(flag){
            val resp = freshchatAPI.agent("Bearer $apiToken",page,items_per_page).execute()
            if (resp.isSuccessful) {
                val r4 = resp.body() ?: throw RuntimeException("freshchat error")
                val j3 =mapper.readTree(r4)
                j3["error"]?.let {
                    throw RuntimeException(it.toString())
                }
                finaljson.add(j3["agents"])
                print("\n page=$page\n")
                if(j3.get("links").has("next_page"))
                    page++
                else
                    flag=false

            }

        }
        return finaljson

    }

    private fun channels(apiToken: String,tableName: String):MutableList<JsonNode> {
        var finaljson = mutableListOf<JsonNode>()
        var flag=true
        var page = 1
        var items_per_page=1
        val resp = freshchatAPI.channel("Bearer $apiToken",items_per_page,page).execute()
        if (resp.isSuccessful) {
            val r4 = resp.body() ?: throw RuntimeException("freshchat code error")
            val j4 = mapper.readTree(r4)
            j4["error"]?.let {
                throw RuntimeException(it.toString())
            }
            finaljson.add(j4["channels"])
            if(j4.get("links").has("next_page"))
                page++
            else
                flag=false
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
        while(flag){
            val resp = freshchatAPI.channel("Bearer $apiToken",page,items_per_page).execute()
            if (resp.isSuccessful) {
                val r4 = resp.body() ?: throw RuntimeException("freshchat error")
                val j3 =mapper.readTree(r4)
                j3["error"]?.let {
                    throw RuntimeException(it.toString())
                }
                finaljson.add(j3["channels"])
                if(j3.get("links").has("next_page"))
                    page++
                else
                    flag=false

            }

        }
        return finaljson

    }

    private fun user(apiToken:String,tableName: String):JsonNode {
        var userid = users.get(0)
        val resp = freshchatAPI.user("Bearer $apiToken",userid).execute()
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
                    FreshchatSourceProcess.logger.error("Problem occurred while fetching Freshchatfrom API")
                }
            }
            throw  RuntimeException("Error at $tableName Table" + resp.errorBody()?.string())
        }

    }

//    private fun conversation(apiToken:String,tableName: String):JsonNode {
//        var conversation_id=""
//        val resp = freshchatAPI.conversation("Bearer $apiToken",conversation_id).execute()
//        if (resp.isSuccessful) {
//            val r4 = resp.body() ?: throw RuntimeException("freshchat code error")
//            val j4 = mapper.readTree(r4)
//            j4["error"]?.let {
//                throw RuntimeException(it.toString())
//            }
//            return  j4
//        }else{
//            when (resp.code()) {
//                401 -> throw RuntimeException("Authentication Failed. " + resp.errorBody()?.string())
//                402 -> throw RuntimeException("Payment Required. " + resp.errorBody()?.string())
//                429 -> {
//                    val responseHeader = resp.headers()
//                    val timeToReset = responseHeader["X-RateLimit-Reset"]?.toLong() ?: 0L
//                    val sleepOffset = Instant.now().epochSecond - timeToReset
//                    TimeUnit.SECONDS.sleep(sleepOffset)
//                }
//                else -> {
//                    FreshchatSourceProcess.logger.error("Problem occurred while fetching Freshchatfrom API")
//                }
//            }
//            throw  RuntimeException("Error at $tableName Table" + resp.errorBody()?.string())
//        }
//
//    }


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

    private fun Emitdata(data: JsonNode, fieldList: List<Column>, tableName: String, emitter: Emitter<Message>, lastRunDate: Long, log: SourceLog?) {
        val lastRunDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastRunDate), ZoneId.systemDefault()).toLocalDate()
        var createdDate = lastRunDate
        var maxCreatedDate = createdDate

        data.map { element ->
            val builder = getMessageBuilder(fieldList, tableName, data)
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
                    ColumnType.INTEGER -> builder.withField(field.name, v.asInt())
                    ColumnType.NUMERIC -> builder.withField(field.name, v.asDouble())
                    ColumnType.BOOLEAN -> builder.withField(field.name, v.asBoolean())
                    ColumnType.DATETIME -> builder.withField(field.name, v.asText())
                    ColumnType.DATE -> builder.withField(field.name, v.asText())
                    ColumnType.RECORD -> {
                        when {
                            v.nodeType == JsonNodeType.ARRAY -> {
                                val childArray = arrayListOf<MutableMap<String, Any>>()
                                v.forEach {
                                    val childNode = mutableMapOf<String, Any>()
                                    val cMap = messageMapGenerator(field.fields, it, childNode)
                                    childArray.add(cMap)
                                }
                                builder.withField(field.name, childArray)
                            }
                            v.nodeType == JsonNodeType.OBJECT -> {
                                val childArray = arrayListOf<MutableMap<String, Any>>()
                                val childMap = mutableMapOf<String, Any>()
                                val cMap = messageMapGenerator(field.fields, v, childMap)
                                childArray.add(cMap)
                                builder.withField(field.name, childArray)
                            }
                            else -> if (field.mode == Mode.REPEATED) {
                                builder.withField(field.name, emptyList<Any>())
                            } else {
                                builder.withField(field.name, value.joinToString())
                            }
                        }
                    }

                    else -> {
                    }
                }
            }
        }
        return builder
    }

    private fun messageMapGenerator(fieldList: List<Column>, element: JsonNode, childMap: MutableMap<String, Any>): MutableMap<String, Any> {
        for (field in fieldList) {
            val value = element[field.name]
            value?.let { v ->
                when (field.type) {
                    ColumnType.STRING -> {
                        if (v.nodeType == JsonNodeType.ARRAY)
                            childMap[field.name] = v.joinToString()
                        else
                            childMap[field.name] = v.asText()
                    }
                    ColumnType.INTEGER -> childMap[field.name] = v.asInt()
                    ColumnType.NUMERIC -> childMap[field.name] = v.asDouble()
                    ColumnType.BOOLEAN -> childMap[field.name] = v.asBoolean()
                    ColumnType.DATETIME -> childMap[field.name] = v.asText()
                    ColumnType.RECORD -> {
                        when {
                            v.nodeType == JsonNodeType.ARRAY -> {
                                val arrayNode = arrayListOf<MutableMap<String, Any>>()
                                v.forEach {
                                    val childNode2 = mutableMapOf<String, Any>()
                                    val temp = messageMapGenerator(field.fields, it, childNode2)
                                    arrayNode.add(temp)
                                }
                                childMap[field.name] = arrayNode
                            }
                            v.nodeType == JsonNodeType.OBJECT -> {
                                val arrayNode = arrayListOf<MutableMap<String, Any>>()
                                val childNode2 = mutableMapOf<String, Any>()
                                val temp = messageMapGenerator(field.fields, v, childNode2)
                                arrayNode.add(temp)
                                childMap[field.name] = arrayNode
                            }
                            else -> childMap[field.name] = v.joinToString()
                        }
                    }
                    else -> {
                    }
                }

            }
        }
        return childMap
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
    //FULL LOAD//
    @GET("v2/groups")
    fun group(
            @Header("Authorization") Authorization: String,
            @Query("page")page:Int,
            @Query("items_per_page")items_per_page: Int
    ): Call<String>
    @GET("/v2/channels ")
    fun channel(
            @Header("Authorization") Authorization: String,
            @Query("items_per_page")items_per_page: Int,
            @Query("page")page: Int
    ): Call<String>
    @GET("v2/users/{user_id}")
    fun user(
            @Header("Authorization") Authorization: String,
            @Path("user_id")userid: String
    ): Call<String>
    @GET("v2/conversations")
    fun conversation(
            @Header("Authorization") Authorization: String,
            @Query("conversation_id") since: String
    ): Call<String>

    //FULL LOAD//
    @GET("v2/agents")
    fun agent(
            @Header("Authorization") Authorization: String,
            @Query("page")page:Int,
            @Query("items_per_page")items_per_page: Int
    ): Call<String>
    @GET("v2/agents")
    fun getuserids(
        @Header("Authorization") Authorization: String

    ): Call<String>


}


private val Uri = ConfigResolver.getPropertyValue("freshchat.url")
private val freshchatAPI = Retrofit.Builder().baseUrl("$Uri/").addConverterFactory(ScalarsConverterFactory.create()).build().create(FreshchatAPI::class.java)


private fun checkDatePass(date1: LocalDateTime, date2: LocalDateTime): Boolean {
    return date1.toLocalDate() < date2.toLocalDate()
}


@Extension
class FreshchatSourceProcessFactory : SourceProcessFactory {
    override fun type() = "FRESHCHAT"

    override fun instance(source: Source): SourceProcess = FreshchatSourceProcess(source)
}
