package kjkrol.kafkademo.producer

import com.fasterxml.jackson.annotation.JsonCreator
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.UUID


@RestController
@RequestMapping(path = arrayOf("producer/video-content"), produces = arrayOf(MediaType.APPLICATION_JSON_VALUE))
internal class VideoContentProducerController(private val videoContentPublisher: VideoContentPublisher) {

    @PostMapping
    fun createVideoContent(@RequestBody request: CreateVideoContentRequest): ResponseEntity<VideoContent> {
        val videoContent = VideoContent(UUID.randomUUID(), request.title)
        videoContentPublisher.publish(videoContent)
        return ResponseEntity(videoContent, HttpStatus.OK)
    }

}

internal data class CreateVideoContentRequest @JsonCreator constructor(var title: String)

