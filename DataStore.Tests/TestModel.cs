using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DataStore.Tests;

[DsSerializerContext(typeof(TestModelJsonSerializerContext))]
public class TestModel: TestBaseModel
{
    [DsIndexedColumn] public Guid? OwnerId { get; set; }
    [DsIndexedColumn] public string ObjectType { get; set; } = string.Empty;
    [DsIndexedColumn] public string Index { get; set; } = string.Empty;
    public Guid? Key { get; set; }
    [DsIndexedColumn] public string FirstName { get; set; } = string.Empty;
    public string MiddleName { get; set; } = string.Empty;
    [DsIndexedColumn] public string LastName { get; set; } = string.Empty;
    public string SortName => StringTools.SortableNameString(FirstName, MiddleName, LastName);
    public string? NullTest { get; set; }
    public string Message { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    [DsIndexedColumn] public DateTimeOffset Dob { get; set; } = DateTimeOffset.MinValue;
    [DsIndexedColumn] public DateTime Dob2 { get; set; } = DateTime.MinValue;
    [DsIndexedColumn] public bool OptIn { get; set; }
    public DateTimeOffset MinDate { get; set; } = DateTimeOffset.MinValue;
    public DateTimeOffset MaxDate { get; set; } = DateTimeOffset.MinValue;
    public int MinInt { get; set; }
    public int MaxInt { get; set; }
    public long MinLong { get; set; }
    public long MaxLong { get; set; }
    public double MinDouble { get; set; }
    public double MaxDouble { get; set; }
    public decimal MinDecimal { get; set; }
    public decimal MaxDecimal { get; set; }
    public TestSubModel Settings { get; set; } = new();
    [DsIndexedColumn("Booger", "Bird")] public Dictionary<string, string> Dictionary { get; set; } = new();
    public Dictionary<string, TestSubModel3> ObjectDictionary { get; set; } = new();
}

public class TestSubModel
{
    public string Option1 { get; set; } = string.Empty;
    public string Option2 { get; set; } = string.Empty;
    public string Option3 { get; set; } = string.Empty;
    public TestSubModel2 Features { get; set; } = new();
}

public class TestSubModel2
{
    [DsIndexedColumn]
    public string Feature1 { get; set; } = string.Empty;
    public string Feature2 { get; set; } = string.Empty;
    public string Feature3 { get; set; } = string.Empty;
    public List<TestSubModel3> Emails { get; set; } = new ();
}

public class TestSubModel3
{
    public string Email { get; set; } = string.Empty;
}

[JsonSerializable(typeof(TestModel))]
[JsonSourceGenerationOptions(WriteIndented = false)]
internal partial class TestModelJsonSerializerContext : JsonSerializerContext
{
}
