:remote connect tinkerpop.server conf/remote.yaml session-managed
:remote config timeout max
system.graph("killrvideo_video_recommendations").
  replication("{'class' : 'SimpleStrategy', 'replication_factor': '1' }").
  systemReplication("{'class' : 'SimpleStrategy', 'replication_factor': '1' }").
  option("graph.schema_mode").set("Production").
  option("graph.allow_scan").set("false").
  option("graph.tx_groups.*.write_consistency").set("LOCAL_QUORUM").
  ifNotExists().
  create()
:remote config alias g killrvideo_video_recommendations.g

// Create property keys
schema.propertyKey("tag").Text().ifNotExists().create();
schema.propertyKey("tagged_date").Timestamp().ifNotExists().create();
schema.propertyKey("userId").Uuid().ifNotExists().create();
schema.propertyKey("email").Text().ifNotExists().create();
schema.propertyKey("added_date").Timestamp().ifNotExists().create();
schema.propertyKey("videoId").Uuid().ifNotExists().create();
schema.propertyKey("name").Text().ifNotExists().create();
schema.propertyKey("description").Text().ifNotExists().create();
schema.propertyKey("preview_image_location").Text().ifNotExists().create();
schema.propertyKey("rating").Int().ifNotExists().create();

// Create vertex labels
schema.vertexLabel("user").partitionKey('userId').properties("userId", "email", "added_date").ifNotExists().create();
schema.vertexLabel("video").partitionKey('videoId').properties("videoId", "name", "description", "added_date", "preview_image_location").ifNotExists().create();
schema.vertexLabel("tag").partitionKey('name').properties("name", "tagged_date").ifNotExists().create();

// Create edge labels
schema.edgeLabel("rated").multiple().properties("rating").connection("user","video").ifNotExists().create();
schema.edgeLabel("uploaded").single().properties("added_date").connection("user","video").ifNotExists().create();
schema.edgeLabel("taggedWith").single().connection("video","tag").ifNotExists().create();

// Edge indexes
schema.vertexLabel("user").index("toVideosByRating").outE("rated").by("rating").ifNotExists().add();
schema.vertexLabel("video").index("toUsersByRating").inE("rated").by("rating").ifNotExists().add();
