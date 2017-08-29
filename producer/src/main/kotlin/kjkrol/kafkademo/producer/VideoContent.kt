package kjkrol.kafkademo.producer

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.UUID

internal data class VideoContent(
        @JsonProperty("id") val id: UUID,
        @JsonProperty("title") val title: String
)