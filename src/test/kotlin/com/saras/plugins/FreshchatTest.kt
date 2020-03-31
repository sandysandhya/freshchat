package com.saras.plugins

import com.fasterxml.jackson.databind.JsonNode
import com.google.gson.GsonBuilder
import com.saras.pipelines.*
import com.saras.pipelines.mock.Initializer
import com.saras.pipelines.mock.Junit5Extension
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import java.io.InputStreamReader
import javax.inject.Inject

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(Junit5Extension::class)
open class FreshchatTest {

    @Inject
    private lateinit var initializer: Initializer

    @Test
    open fun test1() {
        val input = Thread.currentThread().contextClassLoader.getResourceAsStream("freshchat_settings.json") ?: throw RuntimeException()
        val settings = GsonBuilder().registerTypeAdapter(Mode::class.java, ModeDeserializer())
            .registerTypeAdapter(ColumnType::class.java, ColumnTypeDeserializer()).registerTypeAdapter(JsonNode::class.java, JsonNodeSerializer())
            .create().fromJson(InputStreamReader(input), SourceSettings::class.java)
        val source = Source()
        source.frequency = "2"
        source.history = ""
        source.name = "Test"
        source.type = "FRESHCHAT"
        source.settings = settings
        initializer.init(source)
    }
}