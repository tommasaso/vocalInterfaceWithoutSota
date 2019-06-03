import com.google.cloud.dialogflow.v2.QueryResult
import com.google.cloud.texttospeech.v1.*
import com.google.protobuf.ByteString

import javax.sound.sampled.*
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream

object Query {

    fun print(queryResult: QueryResult) {
        println("====================")
        System.out.format("Query Text: '%s'\n", queryResult.queryText)
        System.out.format( "Detected Intent: %s (confidence: %f)\n",
            queryResult.intent.displayName, queryResult.intentDetectionConfidence )
        System.out.format("Fulfillment Text: '%s'\n", queryResult.fulfillmentText)
    }

    fun synthesize(queryResult: QueryResult, textToSpeechClient: TextToSpeechClient?) {
        try {
            // Set the text input to be synthesized
            val input = SynthesisInput.newBuilder()
                .setText(queryResult.fulfillmentText)
                .build()

            // Build the voice request, select the language code ("en-US") and the ssml voice gender
            // ("neutral")
            val voice = VoiceSelectionParams.newBuilder()
                .setLanguageCode("en-GB")
                .setName("en-GB-Wavenet-C")
                .setSsmlGender(SsmlVoiceGender.FEMALE)
                .build()

            // Select the type of audio file you want returned
            val audioConfig = AudioConfig.newBuilder()
                .setAudioEncoding(AudioEncoding.LINEAR16)
                .build()

            // Perform the text-to-speech request on the text input with the selected voice parameters and
            // audio file type
            val responseTTS = textToSpeechClient?.synthesizeSpeech(input, voice, audioConfig)

            // Get the audio contents from the response

            val audioContents = responseTTS?.audioContent

            val out = FileOutputStream("output.wav")
            out.write(audioContents?.toByteArray())
            val file = File(System.getProperty("user.dir") + "/output.wav")

            val stream = AudioSystem.getAudioInputStream(file)
            val format = stream.format
            val info = DataLine.Info(Clip::class.java, format)
            val clip = AudioSystem.getLine(info) as Clip
            clip.open(stream)
            clip.start()
            while (clip.microsecondLength != clip.microsecondPosition) { }

        } catch (e: Exception) {
            println(e)
        }
    }
}
