//Kafka Producer Setting in dot net 

#region Controller

[HttpPost("AddWarehouse")]
public async Task<IActionResult> AddWarehouse([FromBody] WarehouseCreateVm model)
{
    try
    {

        FluentValidation.Results.ValidationResult validationResult = new WarehouseCreateValidator().Validate(model);
        if (validationResult.IsValid)
        {

			var res = await _iRepository.AddWarehouse(model, _loggedInUserId);

			if (res != null && res.StatusCode == StatusCodes.Status200OK)
			{
                await _kafkaProducerService.ProduceAsync(res.Data, KafkaTopicManager.CREATE_WAREHOUSE_PRODUCE_TOPIC);
                return Ok(res);
			}

            return StatusCode(StatusCodes.Status409Conflict, res);

        }
        else
        {
            return StatusCode(StatusCodes.Status409Conflict,
                 new ResponseModel
                 {
                     IsSuccess = false,
                     StatusCode = StatusCodes.Status409Conflict,
                     Status = "Error",
                     Message = "Validation Failed!",
                     Data = FluentValidationHelper.GetErrorMessage(validationResult.Errors)
                 }
                );

        }

    }
    catch (Exception ex)
    {
        return StatusCode(StatusCodes.Status500InternalServerError, Utilities.GetInternalServerErrorMsg(ex));
    }
}

#endregion


#region KafkaTopicManager

public static class KafkaTopicManager
{
    public const string CREATE_WAREHOUSE_PRODUCE_TOPIC = "CREATE_WAREHOUSE_PRODUCE_TOPIC";
    public const string UPDATE_WAREHOUSE_PRODUCE_TOPIC = "UPDATE_WAREHOUSE_PRODUCE_TOPIC";
    public const string MODIFY_WAREHOUSE_PRODUCE_TOPIC = "MODIFY_WAREHOUSE_PRODUCE_TOPIC";
}

#endregion

#region Appsettings
  "KafkaConfig": {
    "BootstrapServers": "192.168.145.176:9092",
    "GroupId": "ADMIN_PRODUCT_SERVICES_GROUP"
  }
#endregion

#region KafkaProducerService

public interface IKafkaProducerService
{
    Task ProduceAsync<T>(T message, string topic);
}

public class KafkaProducerService : IKafkaProducerService
{
    private readonly string _bootstrapServers;

    public KafkaProducerService(IConfiguration configuration)
    {
        var kafkaConfig = configuration.GetSection("KafkaConfig").Get<KafkaConfig>();
        _bootstrapServers = kafkaConfig.BootstrapServers ?? throw new ArgumentNullException(nameof(kafkaConfig.BootstrapServers));
    }

    public async Task ProduceAsync<T>(T message, string topic)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        var jsonMessage = SerializeMessage(message);

        var kafkaMessage = new Message<Null, string> { Value = jsonMessage };

        try
        {
            await producer.ProduceAsync(topic, kafkaMessage);
            producer.Flush(TimeSpan.FromSeconds(10)); // Ensure all messages are sent before exiting
        }
        catch (ProduceException<Null, string> ex)
        {
            // Handle the exception (log, retry, etc.)
            Console.WriteLine($"Error producing message: {ex.Error.Reason}");
            throw;
        }
    }

    private string SerializeMessage<T>(T message)
    {
        return JsonConvert.SerializeObject(message, new JsonSerializerSettings
        {
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore
        });
    }
}

#endregion




