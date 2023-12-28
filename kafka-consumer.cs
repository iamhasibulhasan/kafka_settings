//Kafka Consumer Setting in dot net 
#region Program.cs

// add this line before "var app = builder.Build();"
builder.Services.AddHostedService<KafkaConsumerService>();

#endregion


#region Controller

#endregion


#region KafkaTopicManager



#endregion

#region Appsettings
  "KafkaConfig": {
    "BootstrapServers": "192.168.145.176:9092",
    "GroupId": "SHOPLOVER_INVENTORY_SERVICES_GROUP"
    //"BootstrapServers": "localhost:9092",
    //"GroupId": "customer_services_group"
  },
#endregion




