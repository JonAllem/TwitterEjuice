<Query Kind="Program">
  <Connection>
    <ID>39c8f572-1885-4a6a-b64a-9b1bdd1056c3</ID>
    <Persist>true</Persist>
    <Driver Assembly="IQDriver" PublicKeyToken="5b59726538a49684">IQDriver.IQDriver</Driver>
    <Provider>Devart.Data.MySql</Provider>
    <CustomCxString>AQAAANCMnd8BFdERjHoAwE/Cl+sBAAAAQSqtpEqMw0qiFVkhIKMGggAAAAACAAAAAAAQZgAAAAEAACAAAAAMHg8SFz046w+1rWnM7AZgputMr7ZETyKcYSYl2xKxkgAAAAAOgAAAAAIAACAAAAD9+QNE/QrKidlI4rI+8Tte6GnxuDiVL9NFNOry6XGYgXAAAACckM7o06+Gm0zsTDH3SMO+0ScylgBaQ70Z2ql28AD1UlCNpIlrsSJ4/1Ur89910wkkQQJVjytqRvTNpau79XAMQe+GQ6sxt8z/zRY0M2+gDFpATJERiJxNrpRdgGKUooeSJ6i/KxZ5z4PzyzIwRuHaQAAAAIkTv0YPxfdenDFXEn4fVtDrU2zNV5t9DakoK8y78iiKqYngO6vHambrSYZwSlS2FLA4/6x3y/adcJxBFaRD7YU=</CustomCxString>
    <DriverData>
      <StripUnderscores>false</StripUnderscores>
      <QuietenAllCaps>false</QuietenAllCaps>
      <ExtraCxOptions>CharSet=utf8mb4</ExtraCxOptions>
    </DriverData>
  </Connection>
</Query>

void Main()
{
	var dateRanges = GetDateRange(new DateTime(2018, 1, 1), new DateTime(2019, 1, 1), 7).ToList();

	foreach (var (start, stop) in dateRanges)
	{
		var filename = $@"D:\Documents\GitHub\E-Juice\Data\Tweets\Tweets-{start.Day}-{start.Month}-{start.Year}.csv";
		Util.WriteCsv(Concatenate(GetTweets(start, stop)), filename);
		Console.WriteLine("Finished 1 tweet query.");
	}

	foreach (var (start, stop) in dateRanges)
	{
		var filename = $@"D:\Documents\GitHub\E-Juice\Data\Users\Users-{start.Day}-{start.Month}-{start.Year}.csv";
		Util.WriteCsv(Concatenate(GetUsers(start, stop)), filename);
		Console.WriteLine("Finished 1 user query.");
	}

	var files = Directory.GetFiles(@"D:\Documents\GitHub\E-Juice\Data\Users\", "Users-*.csv");
	var userDict = new Dictionary<string, string>();
	foreach (var user in files.SelectMany(x => MyExtensions.ReadCsv(x).Skip(1)))
	{
		var (id, screenName) = (user[0], user[2]);
		if (!userDict.ContainsKey(id))
		{
			userDict[id] = screenName;
		}
	}
	Util.WriteCsv(userDict, @"D:\Documents\GitHub\E-Juice\Data\Users\AllUsers.csv");
}

public List<string> GetKeywords()
{
	return new List<string> { "e-juices", "e-liquids", " e juice", " e liquids" };
}

public IEnumerable<(DateTime Start, DateTime Stop)> GetDateRange(DateTime start, DateTime stop, int numDaysInterval)
{
	DateTime current = start.AddDays(numDaysInterval);
	while (current < stop)
	{
		yield return (Start: start, Stop: current);
		start = current;
		current = current.AddDays(numDaysInterval);
	}
	if (current != stop)
	{
		yield return (Start: start, Stop: stop);
	}
}

// Define other methods and classes here
public IEnumerable<IQueryable<Tweet>> GetTweets(DateTime startDate, DateTime endDate, int? limit = null)
{
	if (startDate > endDate)
	{
		throw new ArgumentException("startDate should be before endDate!");
	}
	var keywordPredicate = PredicateBuilder.False<Tweets>();
	foreach (var keyword in GetKeywords())
	{
		keywordPredicate = keywordPredicate.Or(f => f.Text.ToLower().Contains(keyword));
	}
	var tweets = Tweets.Where(x => x.IsRetweet == 0 && x.CreatedAt > startDate && x.CreatedAt < endDate)
					.Where(keywordPredicate)
					.Select(x => new Tweet {Id=x.Id, CreatedAt=x.CreatedAt, Text=x.Text, UserId=x.UserId});
	if (limit.HasValue)
	{
		tweets = tweets.Take(limit.Value);
	}
	if (startDate.Year >= 2018 || (startDate.Year == 2017 && startDate.Month == 12))
	{
		yield return tweets;
	}
	if (startDate.Year < 2018)
	{
		var keywordPredicate2 = PredicateBuilder.False<Tweets255>();
		foreach (var keyword in GetKeywords())
		{
			keywordPredicate2 = keywordPredicate2.Or(f => f.Text.ToLower().Contains(keyword));
		}
		var tweets2 = Tweets255s.Where(x => x.IsRetweet == 0 && x.CreatedAt > startDate && x.CreatedAt < endDate)
						.Where(keywordPredicate2)
						.Select(x => new Tweet {Id=x.Id, CreatedAt=x.CreatedAt, Text=x.Text, UserId=x.UserId});
		if (limit.HasValue)
		{
			tweets2 = tweets2.Take(limit.Value);
		}
		yield return tweets2;
	}
}

public IEnumerable<IQueryable<User>> GetUsers(DateTime startDate, DateTime endDate, int? limit = null)
{
	var userQueries = GetTweets(startDate, endDate, limit).Select(q => q.Select(x => x.UserId).Distinct()
					.Join(Twitter_profiles, x => x, y => y.UserId, (x, y) => new User {
						UserId = x, Name = y.Name, ScreenName = y.Screenname,
						FriendsCount = y.FriendsCount, FollowersCount = y.FollowersCount,
						Description = y.Description
					})
				);
	return userQueries;
}

public static IEnumerable<T> Concatenate<T>(IEnumerable<IEnumerable<T>> lists)
{
    return lists.SelectMany(x => x);
}

public class Tweet
{
	public string Id;
	public DateTime? CreatedAt;
	public string Text;
	public string UserId;
}

public class User
{
	public string UserId;
	public string Name;
	public string ScreenName;
	public int? FriendsCount;
	public int? FollowersCount;
	public string Description;
}