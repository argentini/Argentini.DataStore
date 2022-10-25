using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using JsonCons.JsonPath;
using Xunit;
using Xunit.Extensions.Ordering;

namespace DataStore.Tests;

[Order(1)]
public class DataStoreTests
{
    #region Constants

    private readonly List<string> _output = new();
    // ReSharper disable once UnusedMember.Local
    private static string SchemaVersion => "3.0.0";
    
    private const string SqlConnectionString =
        "server=sqlserver;database=datastore;user id=sa;password='P@ssw0rdz!';Trust Server Certificate = true;Connection Timeout=10";

    private const int WriteNumber = 1000; // Must be 200+ and be divisible by ten.
    private const int PerformanceWriteNumber = 2000; // Must be 200+ and be divisible by PerformanceBatchNumber.

    private readonly DataStoreSettings _DataStoreSettings = new()
    {
        SqlConnectionString = SqlConnectionString
    };

    #endregion

    #region Locals

    private readonly TestModel _dso;
    private readonly Guid _ownerId = Guid.NewGuid();
    private string _firstName;
    private string _middleName;
    private string _lastName;
    private string _fullName;
    private readonly Guid? _g = Guid.NewGuid();
    private readonly DateTime _dob2 = Faker.Identification.DateOfBirth();

    #endregion

    #region Constructor

    public DataStoreTests()
    {
        _firstName = Faker.Name.First();
        _middleName = Faker.Name.Middle();
        _lastName = Faker.Name.Last();
        _fullName = StringTools.SortableNameString(_firstName, _middleName, _lastName);

        _dso = new TestModel
        {
            ObjectType = "TestModel",
            Sort = 5,
            OwnerId = _ownerId,
            Key = _g,
            FirstName = _firstName,
            MiddleName = _middleName,
            LastName = _lastName,
            Index = "1",
            NullTest = null,
            Message = @"Now is the time ""for"" all good men!!",
            Phone = Faker.Phone.Number(),
            Email = _fullName.ToSlugString() + "@example.com",
            Dob = new DateTimeOffset(1970, 6, 1, 0, 0, 0, TimeSpan.FromHours(5)),
            Dob2 = _dob2,
            OptIn = Faker.Boolean.Random(),
            MinDate = DataStore.DateTimeOffsetMinValue,
            MaxDate = DataStore.DateTimeOffsetMaxValue,
            MinInt = DataStore.IntMinValue,
            MaxInt = DataStore.IntMaxValue,
            MinLong = DataStore.LongMinValue,
            MaxLong = DataStore.LongMaxValue,
            MinDouble = DataStore.DoubleMinValue,
            MaxDouble = DataStore.DoubleMaxValue,
            MinDecimal = DataStore.DecimalMinValue,
            MaxDecimal = DataStore.DecimalMaxValue,
            Settings = new TestSubModel
            {
                Option1 = "value1",
                Option2 = "value2",
                Option3 = "value3",
                Features = new TestSubModel2
                {
                    Feature1 = "value1",
                    Feature2 = "value2",
                    Feature3 = "value3",
                    Emails = new List<TestSubModel3>
                    {
                        new () { Email = _fullName.ToSlugString() + "1@example.com" },
                        new () { Email = _fullName.ToSlugString() + "2@example.com" },
                        new () { Email = _fullName.ToSlugString() + "3@example.com" },
                    }
                }
            },
            Dictionary = new Dictionary<string, string>
            {
                { "Bird", "animal" },
                { "Rock", "geology" },
                { "Booger", "food" }
            },
            ObjectDictionary = new Dictionary<string, TestSubModel3>
            {
                { "Email1", new () { Email = _fullName.ToSlugString() + "1@example.com" } },
                { "Email2", new () { Email = _fullName.ToSlugString() + "2@example.com" } },
                { "Email3", new () { Email = _fullName.ToSlugString() + "3@example.com" } }
            }
        };
    }

    #endregion

    private async Task CloneAsUnique(TestModel dso, int index = 0)
    {
        await _dso.CloneObjectToAsync(dso);
        
        _firstName = Faker.Name.First();
        _middleName = Faker.Name.Middle();
        _lastName = Faker.Name.Last();
        _fullName = StringTools.SortableNameString(_firstName, _middleName, _lastName);

        dso.Id = Guid.NewGuid();
        dso.Index = index.ToString();
        dso.Sort = index;
        dso.FirstName = _firstName;
        dso.LastName = _lastName;
        dso.Email = _fullName.ToSlugString() + index + "@example.com";
        dso.OptIn = Faker.Boolean.Random();
    }

    private void PerformanceOutput(DataStore dataStore)
    {
        var totalTime = dataStore.LastTotalTimeMs;
        var totalReadTime = dataStore.LastTotalReadTimeMs;
        var totalWriteTime = dataStore.LastTotalWriteTimeMs;
        var totalSqlTime = totalReadTime + totalWriteTime;
        var overhead = totalTime - totalSqlTime;
        var efficiency = (100 / (totalTime / totalSqlTime)) * 0.01;

        var sqlTime =
            $"   Total SQL Time       : {StringTools.FormatTimerString(totalSqlTime)}  /  Read: {StringTools.FormatTimerString(totalReadTime)}  /  Write: {StringTools.FormatTimerString(totalWriteTime)}";
        var cpuTime =
            $"   Total CPU Time       : {StringTools.FormatTimerString(overhead)}";
        
        if (dataStore.LastTotalWriteTimeMs == 0)
        {
            sqlTime =
                $"   Total SQL Time       : {StringTools.FormatTimerString(totalSqlTime)}";
            cpuTime =
                $"   Total CPU Time       : {StringTools.FormatTimerString(overhead)}";
        }
        
        _output.Add($"");
        _output.Add(sqlTime);
        _output.Add(cpuTime);
        _output.Add($"   Total Time           : {StringTools.FormatTimerString(totalTime)}");
        _output.Add($"   Efficiency           : {efficiency:P1}");
        _output.Add($"   Effective Rate       : {StringTools.Performance(PerformanceWriteNumber, totalTime, 0)}");
    }
    
    [Fact, Order(0)]
    public async Task Initialization()
    {
        await DataStore.DeleteDatabaseObjectsAsync(SqlConnectionString);

        var dataStore = new DataStore(_DataStoreSettings);

        dataStore.ShrinkDatabase();
        
        Assert.NotNull(dataStore);
        Assert.NotNull(dataStore.Settings);
        Assert.NotEmpty(dataStore.TableDefinitions);
    }

    [Fact, Order(1)]
    public async Task ObjectHandling()
    {
        var types = ObjectTools.GetInheritedTypes(typeof(DsObject)).ToList();
        Assert.True(types.Any());

        Assert.Equal("TestBaseModel", DataStore.GenerateTableName(typeof(TestBaseModel)));
        Assert.Equal("TestBaseModel_TestModel", DataStore.GenerateTableName(typeof(TestModel)));
        Assert.Equal("TestBaseModel_TestModel_TestLineageModel", DataStore.GenerateTableName(typeof(TestLineageModel)));
        
        Assert.Equal("$.Id", DsObject.GetJsonPropertyPath<TestModel>(p => p.Id));
        Assert.Equal("$.Settings.Features.Feature1", DsObject.GetJsonPropertyPath<TestModel>(p => p.Settings.Features.Feature1));
        Assert.Equal("$.Settings.Features.Emails[*]", DsObject.GetJsonPropertyPath<TestModel>(d => d.Settings.Features.Emails));
        Assert.Equal("$.Settings.Features.Emails[*].Email", DsObject.GetJsonPropertyPath<TestModel, TestSubModel3>(p => p.Settings.Features.Emails, e => e.Email));
        Assert.Equal("$.Dictionary", DsObject.GetJsonPropertyPath<TestModel>(p => p.Dictionary));
        
        Assert.Equal(_dso.Settings.Features.Feature1, _dso.Value<TestModel, string>(p => p.Settings.Features.Feature1));
        Assert.Equal(_dso.MaxInt, _dso.Value<TestModel, int>(p => p.MaxInt));
        Assert.Equal(_dso.MaxDecimal, _dso.Value<TestModel, decimal>(p => p.MaxDecimal));
        Assert.Equal(_dso.Dictionary["Booger"], _dso.Value<TestModel, string>(p => p.Dictionary["Booger"]));
        Assert.Equal(_dso.Dob, _dso.Value<TestModel, DateTimeOffset>(p => p.Dob));
        Assert.Equal(_dso.OptIn, _dso.Value<TestModel, bool>(p => p.OptIn));
        Assert.Equal(3, _dso.Values<TestModel, TestSubModel3, string>(p => p.Settings.Features.Emails, q => q.Email).Count);
        Assert.Equal(3, _dso.Values<TestModel, string>(p => p.Dictionary).Count);
        Assert.Equal("animal", _dso.Values<TestModel, string>(p => p.Dictionary)[0]);
        Assert.Equal(_dso.Settings.Features.Emails[0].Email, _dso.Values<TestModel,TestSubModel3,string>(d => d.Settings.Features.Emails, e => e.Email)[0]);
        Assert.Equal(3, _dso.Values<TestModel,TestSubModel3>(d => d.Settings.Features.Emails).Count);
        Assert.Equal("food", _dso.Value<TestModel, Dictionary<string,string>>(p => p.Dictionary)?["Booger"]);

        var clone = new TestModel();
        
        await _dso.CloneObjectToAsync(clone);

        Assert.True(clone.SameAsObject(_dso));

        #region DsObject Inequality Tests
        
        clone.Dob = DateTimeOffset.UtcNow;
        Assert.False(clone.SameAsObject(_dso));
        await _dso.CloneObjectToAsync(clone);

        clone.Dob2 = DateTime.UtcNow;
        Assert.False(clone.SameAsObject(_dso));
        await _dso.CloneObjectToAsync(clone);
        
        clone.Dictionary["Booger"] = "gross";
        Assert.False(clone.SameAsObject(_dso));
        await _dso.CloneObjectToAsync(clone);

        clone.Settings.Features.Feature1 = "Bad";
        Assert.False(clone.SameAsObject(_dso));
        await _dso.CloneObjectToAsync(clone);

        clone.Settings.Features.Emails[0].Email = "Bad";
        Assert.False(clone.SameAsObject(_dso));
        await _dso.CloneObjectToAsync(clone);

        Assert.True(clone.SameAsObject(_dso));
        
        #endregion
        
        var sourceDsQuery = new DsQuery()
            .StringProp<TestModel>(c => c.ObjectType).EqualTo("TestModel")
            .AND()
            .StringProp<TestModel>(c => c.Index).EndsWith("1")
            .AND()
            .StringProp<TestModel>(c => c.ObjectType).EqualTo("TestModel")
            .AND()
            .StringProp<TestModel>(c => c.ObjectType).EqualToProp<TestModel>(p => p.ObjectType)
            .AND()
            .NOT().StringProp<TestModel>(c => c.Index).EndsWith("2")
            .AND()
            .NotBegin()
                .StringProp<TestModel>(c => c.Index).EndsWith("2")
            .NotEnd()
            .AND()
            .NumberProp<TestModel,int>(c => c.Sort).GreaterThan(0)
            .AND()
            .NumberProp<TestModel,decimal>(c => c.MaxDecimal).GreaterThan(0)
            .AND()
            .StringProp<TestModel>(c => c.Settings.Features.Feature1).EqualTo("value1")
            .AND()
            .StringProp<TestModel>(c => c.Dictionary["Booger"]).EqualTo("food")
            .AND()
            .StringProp<TestModel, TestSubModel3>(c => c.ObjectDictionary["Email1"], e => e.Email).EndsWith("1@example.com")
            .AND()
            .GroupBegin()
                .StringProp<TestModel,TestSubModel3>(c => c.Settings.Features.Emails, e => e.Email).Contains("1")
                .AND()
                .StringProp<TestModel,TestSubModel3>(c => c.Settings.Features.Emails, e => e.Email).EndsWith("1@example.com")
            .GroupEnd()
            .AND()
            .StringProp<TestModel>(c => c.Settings.Features.Feature1).EqualTo("value1");
        
        var cloneDsQuery = new DsQuery();

        await sourceDsQuery.CloneObjectToAsync(cloneDsQuery); 
        
        Assert.True(cloneDsQuery.SameAsObject(sourceDsQuery));
        Assert.True(cloneDsQuery.WhereClause.SameAsObject(sourceDsQuery.WhereClause));

        clone.Serialize(generateJsonDocument: false);

        var jsonObject = JsonNode.Parse(clone.Json);

        Assert.NotNull(jsonObject);
        Assert.Equal(clone.Key, jsonObject?["Key"]?.GetValue<Guid>());
        Assert.Equal(clone.Key.ToString(), jsonObject?["Key"]?.GetValue<string>());
        Assert.Equal(clone.Settings.Features.Feature1, jsonObject?["Settings"]?["Features"]?["Feature1"]?.GetValue<string>());
        Assert.Equal(clone.Settings.Features.Feature1, JsonSelector.Select(jsonObject.Deserialize<JsonElement>(), "$.Settings.Features.Feature1")[0].GetString());
        Assert.Equal(3, jsonObject?["Settings"]?["Features"]?["Emails"]?.AsArray().Count);

        jsonObject!["Settings"]!.AsObject()["Features"]!.AsObject()["Feature1"] = "NEW VALUE";
            
        Assert.NotEqual(clone.Settings.Features.Feature1, jsonObject["Settings"]?["Features"]?["Feature1"]?.GetValue<string>());

        clone.Settings.Features.Feature1 = "NEW VALUE";
        
        Assert.Equal(clone.Settings.Features.Feature1, jsonObject["Settings"]?["Features"]?["Feature1"]?.GetValue<string>());
        
        Assert.NotNull(jsonObject);
    }
    
    [Fact, Order(2)]
    public async Task SimpleReadWrite()
    {
        await DataStore.DeleteDatabaseObjectsAsync(SqlConnectionString);
        
        var dataStore = new DataStore(_DataStoreSettings);
        var _dsoId = _dso.Id;
        
        Assert.True(_dso.IsNew);        
        
        await dataStore.SaveAsync(_dso);

        Assert.False(_dso.IsNew);
        Assert.Equal(1, await dataStore.GetCountAsync<TestModel>());
        
        var dsq = new DsQuery()
            .StringProp<TestModel>(c => c.ObjectType).EqualTo("TestModel")
            .AND()
            .NumberProp<int>("MaxInt").EqualTo(DataStore.IntMaxValue)
            .AND()
            .GroupBegin()
                .StringProp("Settings.Features.Emails.Email").Contains("2")
                .AND()
                .StringProp("Settings.Features.Emails.Email").EndsWith("2@example.com")
                .AND()
                .StringProp("Dictionary.Booger").EqualTo("food")
            .GroupEnd()
            .AND()
            .StringProp("Settings.Features.Feature1").EqualTo("value1");

        var users = await dataStore.GetManyAsync<TestModel>(1, 50,
                dsq,
            new DsOrderBy()
                .Prop("LastName").Ascending()
        );

        Assert.Single(users);

        dsq = new DsQuery()
            .StringProp<TestModel>(c => c.ObjectType).EqualTo("TestModel")
            .AND()
            .NumberProp<TestModel,int>(c => c.MaxInt).EqualTo(DataStore.IntMaxValue)
            .AND()
            .GroupBegin()
                .StringProp<TestModel, TestSubModel3>(c => c.Settings.Features.Emails, e => e.Email).Contains("2")
                .AND()
                .StringProp<TestModel, TestSubModel3>(c => c.Settings.Features.Emails, e => e.Email).EndsWith("2@example.com")
                .AND()
                .StringProp<TestModel>(c => c.Dictionary["Booger"]).EqualTo("food")
            .GroupEnd()
            .AND()
            .StringProp<TestModel>(c => c.Settings.Features.Feature1).EqualTo("value1");
        
        users = await dataStore.GetManyAsync<TestModel>(
            1, 
            50,
            dsq,
            new DsOrderBy()
                .Prop<TestModel>(p => p.LastName).Ascending()
        );

        Assert.Single(users);
        
        users = await dataStore.GetManyAsync<TestModel>(1, 50, new DsQuery().StringProp<TestModel>(c => c.ObjectType).EqualTo("TestModel"));

        Assert.Single(users);

        var user = await dataStore.GetSingleByIdAsync<TestModel>(_dsoId);

        Assert.NotNull(user);
        Assert.Equal(_dsoId, user?.Id);

        dataStore.Purge<TestModel>(dsq);
            
        users = await dataStore.GetManyAsync<TestModel>(1, 50, dsq);

        Assert.Empty(users);
    }

    [Fact, Order(3)]
    public async Task DeleteUndelete()
    {
        await DataStore.DeleteDatabaseObjectsAsync(SqlConnectionString);
        
        var dataStore = new DataStore(_DataStoreSettings);

        var dso1 = new TestModel();
        var dso2 = new TestModel();
        var dso3 = new TestModel();

        await CloneAsUnique(dso1, 1);
        await CloneAsUnique(dso2, 2);
        await CloneAsUnique(dso3, 3);

        await dataStore.SaveManyAsync(new [] { dso1, dso2, dso3 });

        Assert.Equal(3, await dataStore.GetCountAsync<TestModel>(new DsQuery().StringProp<TestModel>(c => c.ObjectType).EqualTo("TestModel")));

        await dataStore.DeleteAsync(dso1);
        
        Assert.Equal(2, await dataStore.GetCountAsync<TestModel>(new DsQuery().StringProp<TestModel>(c => c.ObjectType).EqualTo("TestModel")));
        
        await dataStore.UndeleteAsync(dso1);
        
        Assert.Equal(3, await dataStore.GetCountAsync<TestModel>(new DsQuery().StringProp<TestModel>(c => c.ObjectType).EqualTo("TestModel")));
    }
    
    [Fact, Order(4)]
    public async Task Lineages()
    {
        await DataStore.DeleteDatabaseObjectsAsync(SqlConnectionString);

        var dataStore = new DataStore(_DataStoreSettings);

        #region Lineage

        var dsos = new List<TestLineageModel>();
        var c = 0;
        var parents = new List<Guid>();

        for (var x = 0; x < WriteNumber; x++)
        {
            string fullName;

            if (c == 4)
            {
                parents.Clear();
                c = 0;
            }

            var newDso = new TestLineageModel();

            await CloneAsUnique(newDso, x);

            newDso.ObjectType = "TestLineageModel";
            
            switch (x)
            {
                case WriteNumber - 3:
                    newDso.FirstName = "Michael";
                    newDso.MiddleName = "Q";
                    newDso.LastName = "Tester1";
                    fullName = StringTools.SortableNameString("Michael", "Q", "Tester1");
                    newDso.Email = fullName.ToSlugString() + "@example.com";
                    break;
                case WriteNumber - 2:
                    newDso.FirstName = "Gwen";
                    newDso.MiddleName = "E";
                    newDso.LastName = "Tester2";
                    fullName = StringTools.SortableNameString("Gwen", "E", "Tester1");
                    newDso.Email = fullName.ToSlugString() + "@example.com";
                    break;
                case WriteNumber - 1:
                    newDso.FirstName = "Chloe";
                    newDso.MiddleName = "I";
                    newDso.LastName = "Tester3";
                    fullName = StringTools.SortableNameString("Chloe", "I", "Tester1");
                    newDso.Email = fullName.ToSlugString() + "@example.com";
                    break;
                default:
                    newDso.Email = _fullName.ToSlugString() + x + "@example.com";
                    break;
            }

            if (c < 4)
            {
                parents.Add(newDso.Id);
            }

            newDso.ParentId = c switch
            {
                1 => parents[0],
                2 => parents[1],
                3 => parents[2],
                _ => newDso.ParentId
            };

            dsos.Add(newDso);

            c++;
        }

        await dataStore.SaveManyAsync(dsos);

        var father = await dataStore.GetSingleAsync(typeof(TestLineageModel), new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestLineageModel").AND().NumberProp<TestModel, int>(s => s.Sort).EqualTo(WriteNumber - 3)) as TestLineageModel;
        var child = await dataStore.GetSingleAsync(typeof(TestLineageModel), new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestLineageModel").AND().NumberProp<int>("Index").EqualTo(WriteNumber - 2)) as TestLineageModel;
        var grandchild = await dataStore.GetSingleAsync(typeof(TestLineageModel), new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestLineageModel").AND().StringProp("Email").StartsWith("tester1-chloe-i")) as TestLineageModel;

        Assert.NotNull(father);
        Assert.Equal(WriteNumber - 3, father!.Sort);
        Assert.Equal("Michael", father.FirstName);

        Assert.NotNull(child);
        Assert.Equal(WriteNumber - 2, child!.Sort);
        Assert.Equal("Gwen", child.FirstName);

        Assert.NotNull(grandchild);
        Assert.Equal(WriteNumber - 1, grandchild!.Sort);
        Assert.Equal("Chloe", grandchild.FirstName);

        #region Generics version

        father = await dataStore.GetSingleAsync<TestLineageModel>(new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestLineageModel").AND().NumberProp<TestLineageModel,int>(p => p.Sort).EqualTo(WriteNumber - 3));
        child = await dataStore.GetSingleAsync<TestLineageModel>(new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestLineageModel").AND().StringProp<TestLineageModel>(p => p.Index).EqualTo((WriteNumber - 2).ToString()));
        grandchild = await dataStore.GetSingleAsync<TestLineageModel>(new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestLineageModel").AND().StringProp<TestLineageModel>(p => p.Email).StartsWith("tester1-chloe-i"));

        Assert.NotNull(father);
        Assert.Equal(WriteNumber - 3, father!.Sort);
        Assert.Equal("Michael", father.FirstName);

        Assert.NotNull(child);
        Assert.Equal(WriteNumber - 2, child!.Sort);
        Assert.Equal("Gwen", child.FirstName);

        Assert.NotNull(grandchild);
        Assert.Equal(WriteNumber - 1, grandchild!.Sort);
        Assert.Equal("Chloe", grandchild.FirstName);
        
        #endregion
        
        #endregion
        
        #region Descendants 
        
        var descendants = await dataStore.GetDescendantsAsync<TestLineageModel>(
            father,
            1, 50,
            new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestLineageModel"),
            new DsOrderBy()
                .DepthAscending()
        );

        Assert.NotNull(descendants);
        Assert.Equal(2, descendants.Count);
        Assert.Equal("Gwen", descendants[0].FirstName);
        Assert.Equal("Chloe", descendants[1].FirstName);

        #endregion
        
        #region Children

        var children = await dataStore.GetChildrenAsync<TestLineageModel>(
            father,
            1, 50,
            new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestLineageModel"),
            new DsOrderBy()
                .DepthAscending()
        );

        Assert.NotNull(children);
        Assert.Single(children);
        Assert.Equal("Gwen", children[0].FirstName);

        #endregion
        
        #region Ancestors

        var ancestors = await dataStore.GetAncestorsAsync<TestLineageModel>(
            grandchild,
            1, 50,
            new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestLineageModel")
        );

        Assert.NotNull(ancestors);
        Assert.Equal(3, ancestors.Count);
        Assert.Equal("Gwen", ancestors[0].FirstName);
        Assert.Equal("Michael", ancestors[1].FirstName);
        
        #endregion
        
        #region Move ancestor

        father.ParentId = null;

        await dataStore.SaveAsync(father);

        ancestors = await dataStore.GetAncestorsAsync<TestLineageModel>(
            grandchild,
            1, 50,
            new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestLineageModel")
        );

        Assert.NotNull(ancestors);
        Assert.Equal(2, ancestors.Count);
        Assert.Equal("Gwen", ancestors[0].FirstName);
        Assert.Equal("Michael", ancestors[1].FirstName);
        
        #endregion
        
        #region Generate All Lineage Data

        await DataStore.DeleteDatabaseObjectsAsync(SqlConnectionString);

        dataStore = new DataStore(_DataStoreSettings);
        
        var dsos2 = new List<TestModel>();
        
        for (var x = 0; x < WriteNumber; x++)
        {
            string fullName;

            if (c == 4)
            {
                parents.Clear();
                c = 0;
            }

            var newDso = new TestModel();

            await CloneAsUnique(newDso, x);

            switch (x)
            {
                case WriteNumber - 3:
                    newDso.FirstName = "Michael";
                    newDso.MiddleName = "Q";
                    newDso.LastName = "Tester1";
                    fullName = StringTools.SortableNameString("Michael", "Q", "Tester1");
                    newDso.Email = fullName.ToSlugString() + "@example.com";
                    break;
                case WriteNumber - 2:
                    newDso.FirstName = "Gwen";
                    newDso.MiddleName = "E";
                    newDso.LastName = "Tester2";
                    fullName = StringTools.SortableNameString("Gwen", "E", "Tester1");
                    newDso.Email = fullName.ToSlugString() + "@example.com";
                    break;
                case WriteNumber - 1:
                    newDso.FirstName = "Chloe";
                    newDso.MiddleName = "I";
                    newDso.LastName = "Tester3";
                    fullName = StringTools.SortableNameString("Chloe", "I", "Tester1");
                    newDso.Email = fullName.ToSlugString() + "@example.com";
                    break;
                default:
                    newDso.Email = _fullName.ToSlugString() + x + "@example.com";
                    break;
            }

            if (c < 4)
            {
                parents.Add(newDso.Id);
            }

            newDso.ParentId = c switch
            {
                1 => parents[0],
                2 => parents[1],
                3 => parents[2],
                _ => newDso.ParentId
            };

            dsos2.Add(newDso);

            c++;
        }

        await dataStore.SaveManyAsync(dsos2);
        dataStore.UpdateAllLineages<TestModel>();

        Assert.False(dsos2[0].IsNew);

        var grandchild2 = await dataStore.GetSingleAsync(typeof(TestModel), new DsQuery().StringProp("Email").StartsWith("tester1-chloe-i")) as TestModel;
        
        var ancestors2 = await dataStore.GetAncestorsAsync<TestModel>(
            grandchild2,
            1, 50,
            new DsQuery().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestModel")
        );

        var getCount = await dataStore.GetCountAsync<TestModel>(new DsQuery().IdInList(grandchild2?.Lineage).AND().StringProp<TestModel>(o => o.ObjectType).EqualTo("TestModel"));
        
        Assert.NotNull(ancestors2);
        Assert.Equal(3, ancestors2.Count);
        Assert.Equal(3, getCount);
        Assert.Equal("Gwen", ancestors2[0].FirstName);
        Assert.Equal("Michael", ancestors2[1].FirstName);

        #endregion
    }
    
    [Fact, Order(5)]
    public async Task Performance()
    {
        var timer = new Stopwatch();
        var totalTimer = new Stopwatch();
        
        await DataStore.DeleteDatabaseObjectsAsync(SqlConnectionString);
        
        var dataStore = new DataStore(_DataStoreSettings);
        
        totalTimer.Start();

        var title = "DataStore Version " + DataStore.Version + " / SQL Server Version " +
                    dataStore.SqlServerVersion + " / " + DateTime.Now.ToLongDateString() + " @ " +
                    DateTime.Now.ToLongTimeString();

        _output.Add(title);
        _output.Add("=".RepeatString(title.Length));
        _output.Add("");
        
        var dsos = new List<TestModel>();
        var query = new DsQuery()
            .StringProp<TestModel>(c => c.ObjectType).EqualTo("TestModel")
            .AND()
            .StringProp<TestModel>(c => c.Index).EndsWith("1")
            .AND()
            .StringProp<TestModel>(c => c.ObjectType).EqualTo("TestModel")
            .AND()
            .StringProp<TestModel>(c => c.ObjectType).RawSql(" = 'TestModel'")
            .AND()
            .StringProp<TestModel>(c => c.ObjectType).EqualToProp<TestModel>(p => p.ObjectType)
            .AND()
            .NOT().StringProp<TestModel>(c => c.Index).EndsWith("2")
            .AND()
            .NotBegin()
                .StringProp<TestModel>(c => c.Index).EndsWith("2")
            .NotEnd()
            .AND()
            .NumberProp<TestModel, int>(c => c.Sort).GreaterThan(0)
            .AND()
            .NumberProp<TestModel, decimal>(c => c.MaxDecimal).GreaterThan(0)
            .AND()
            .StringProp<TestModel>(c => c.Settings.Features.Feature1).EqualTo("value1")
            .AND()
            .StringProp<TestModel>(c => c.Dictionary["Booger"]).EqualTo("food")
            .AND()
            .StringProp<TestModel>(c => c.Settings.Features.Feature1).EqualTo("value1")
            .AND()
            .DateTimeProp<TestModel>(p => p.Dob).Before(DateTimeOffset.UtcNow);

        _output.Add($"PERFORMANCE FOR {PerformanceWriteNumber:N0} OBJECTS");
        _output.Add("");

        for (var x = 0; x < PerformanceWriteNumber; x++)
        {
            var newDso = new TestModel();
            
            await CloneAsUnique(newDso, x);            
            
            dsos.Add(newDso);
        }

        timer.Start();

        for (var x = 0; x < PerformanceWriteNumber; x++) dsos[x].Serialize(dataStore, generateJsonDocument: false);

        _output.Add($"Serialize Baseline      : {StringTools.FormatTimerString(timer.ElapsedMilliseconds)}");

        timer.Reset();
        timer.Start();

        for (var x = 0; x < PerformanceWriteNumber; x++) await dsos[x].Deserialize(dsos[x].Json, dataStore);

        _output.Add($"Deserialize Baseline    : {StringTools.FormatTimerString(timer.ElapsedMilliseconds)}");

        #region No Lineages

        await DataStore.DeleteDatabaseObjectsAsync(SqlConnectionString);
        
        dataStore = new DataStore(_DataStoreSettings);
        
        timer.Reset();
        timer.Start();

        await dataStore.SaveManyAsync(dsos);

        dataStore.LastTotalTimeMs = dataStore.LastTotalTimeMs;
        dataStore.LastTotalReadTimeMs = dataStore.LastTotalReadTimeMs;
        dataStore.LastTotalWriteTimeMs = dataStore.LastTotalWriteTimeMs;

        timer.Stop();
        
        _output.Add($"");
        _output.Add($"=> SAVES");
        PerformanceOutput(dataStore);
        
        timer.Reset();
        timer.Start();
        
        var users = await dataStore.GetManyAsync<TestModel>(
            1, PerformanceWriteNumber,
            query,
            new DsOrderBy()
                .Prop<TestModel>(p => p.Index).Ascending()
        );

        timer.Stop();

        _output.Add($"");
        _output.Add($"=> READS");
        PerformanceOutput(dataStore);
        
        Assert.NotNull(users);
        Assert.Equal(PerformanceWriteNumber / 10, users.Count);

        #endregion

        var counter = 0;
        var parents = new List<Guid>();
        
        var query2 = new DsQuery()
            .StringProp<TestModel>(c => c.ObjectType).EqualTo("TestLineageModel")
            .AND()
            .StringProp<TestLineageModel>(c => c.Index).EndsWith("1")
            .AND()
            .StringProp<TestLineageModel>(c => c.ObjectType).EqualTo("TestLineageModel")
            .AND()
            .StringProp<TestLineageModel>(c => c.ObjectType).RawSql(" = 'TestLineageModel'")
            .AND()
            .StringProp<TestLineageModel>(c => c.ObjectType).EqualToProp<TestLineageModel>(p => p.ObjectType)
            .AND()
            .NOT().StringProp<TestLineageModel>(c => c.Index).EndsWith("2")
            .AND()
            .NotBegin()
                .StringProp<TestLineageModel>(c => c.Index).EndsWith("2")
            .NotEnd()
            .AND()
            .NumberProp<TestLineageModel, int>(c => c.Sort).GreaterThan(0)
            .AND()
            .NumberProp<TestLineageModel, decimal>(c => c.MaxDecimal).GreaterThan(0)
            .AND()
            .StringProp<TestLineageModel>(c => c.Settings.Features.Feature1).EqualTo("value1")
            .AND()
            .StringProp<TestLineageModel>(c => c.Dictionary["Booger"]).EqualTo("food")
            .AND()
            .StringProp<TestLineageModel>(c => c.Settings.Features.Feature1).EqualTo("value1")
            .AND()
            .DateTimeProp<TestLineageModel>(p => p.Dob).Before(DateTimeOffset.UtcNow);
        
        var dsos2 = new List<TestLineageModel>();
        
        for (var x = 0; x < PerformanceWriteNumber; x++)
        {
            if (counter == 4)
            {
                parents.Clear();
                counter = 0;
            }
            
            var newDso = new TestLineageModel();

            await CloneAsUnique(newDso, x);            
            
            newDso.ObjectType = "TestLineageModel";

            if (counter < 4)
            {
                parents.Add(newDso.Id);
            }

            newDso.ParentId = counter switch
            {
                1 => parents[0],
                2 => parents[1],
                3 => parents[2],
                _ => newDso.ParentId
            };
            
            dsos2.Add(newDso);

            counter++;
        }

        #region Lineages
        
        await DataStore.DeleteDatabaseObjectsAsync(SqlConnectionString);
        
        dataStore = new DataStore(_DataStoreSettings);
        
        timer.Reset();
        timer.Start();

        await dataStore.SaveManyAsync(dsos2);

        dataStore.LastTotalTimeMs = dataStore.LastTotalTimeMs;
        dataStore.LastTotalReadTimeMs = dataStore.LastTotalReadTimeMs;
        dataStore.LastTotalWriteTimeMs = dataStore.LastTotalWriteTimeMs;

        timer.Stop();

        _output.Add($"");
        _output.Add($"=> SAVES (lineages)");
        PerformanceOutput(dataStore);
        
        timer.Reset();
        timer.Start();
        
        var users2 = await dataStore.GetManyAsync<TestLineageModel>(
            1, PerformanceWriteNumber,
            query2,
            new DsOrderBy()
                .Prop<TestLineageModel>(p => p.Index).Ascending()
        );

        timer.Stop();

        _output.Add($"");
        _output.Add($"=> READS (lineages)");
        PerformanceOutput(dataStore);
        
        Assert.NotNull(users2);
        Assert.Equal(PerformanceWriteNumber / 10, users2.Count);

        #endregion

        totalTimer.Stop();

        _output.Add($"");
        _output.Add($"=> TOTAL TEST TIME: " + StringTools.FormatTimerString(totalTimer.ElapsedMilliseconds));
        
        _output.Add("");
        await File.AppendAllLinesAsync("../../../results.txt", _output);
    }
}
