using System.Text.Json.Serialization;

namespace DataStore.Tests;

[DsUseLineageFeatures]
[DsSerializerContext(typeof(TestLineageModelJsonSerializerContext))]
public class TestLineageModel: TestModel
{
}

[JsonSerializable(typeof(TestLineageModel))]
[JsonSourceGenerationOptions(WriteIndented = false)]
internal partial class TestLineageModelJsonSerializerContext : JsonSerializerContext
{
}
