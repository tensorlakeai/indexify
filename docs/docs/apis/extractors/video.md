# Video Extractors

Video extractors are specialized tools designed to process and analyze video content, extracting valuable information from the visual and audio components of video files. These extractors can perform various tasks such as frame extraction, audio separation, face detection, and object tracking. By breaking down videos into their constituent parts or identifying specific elements within them, video extractors enable advanced analysis, indexing, and content understanding for a wide range of applications in media processing, surveillance, and content management systems. If you want to learn more about extractors, their design and usage, read the Indexify [documentation](https://docs.getindexify.ai/concepts/).

| Extractor Name | Use Case | Supported Input Types |
|----------------|----------|------------------------|
| Key Frame Extractor | Extract key frames from videos | video, video/mp4 |
| Audio Extractor | Extract audio from video files | video, video/mp4, video/mov, video/avi |
| Face Extractor | Extract and cluster unique faces from videos | video, video/mp4 |
| Track Extractor | Track objects in videos using YOLO | video, video/mp4 |

## [Key Frame Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/keyframes)

### Description
Dissect a video into its key frames and generate JPEG image content for each extracted frame. Comes with frame frequency and scene detection configuration that can be changed with input parameters.

### Input Data Types
```
["video", "video/mp4"]
```

### Class Name
```
KeyFrameExtractor
```

### Download Command
=== "Bash"
```bash
indexify-extractor download tensorlake/scene-frame-extractor
```

## [Audio Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/audio-extractor)

### Description
Extract the audio from a video file.

### Input Data Types
```
["video", "video/mp4", "video/mov", "video/avi"]
```

### Class Name
```
AudioExtractor
```

### Download Command
=== "Bash"
```bash
indexify-extractor download tensorlake/audio-extractor
```

## [Face Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/face-extractor)

### Description
Extract unique faces from video. This extractor uses face_detection to locate and extract facial features and sklearn DBSCAN to cluster and find uniqueness.

### Input Data Types
```
["video", "video/mp4"]
```

### Class Name
```
FaceExtractor
```

### Download Command
=== "Bash"
```bash
indexify-extractor download tensorlake/face-extractor
```

## [Track Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/tracking)

### Description
A YOLO-based object tracker for video.

### Input Data Types
```
["video", "video/mp4"]
```

### Class Name
```
TrackExtractor
```

### Download Command
=== "Bash"
```bash
indexify-extractor download tensorlake/tracking
```
