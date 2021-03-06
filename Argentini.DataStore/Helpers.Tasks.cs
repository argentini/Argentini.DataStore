namespace Argentini.DataStore;

public static class Tasks
{
	/// <summary>
	/// Wait for a Task object to complete, be cancelled, or be faulted.
	/// </summary>
	/// <param name="task"></param>
	/// <param name="sleepMs"></param>
	/// <returns></returns>
	/// <exception cref="AggregateException"></exception>
	// ReSharper disable once UnusedMember.Global
	public static void WaitForTask(Task task, int sleepMs = 25)
	{
		while (task.IsCompleted == false && task.IsCanceled == false && task.IsFaulted == false)
		{
			Thread.Sleep(sleepMs);
		}

		if (task.IsFaulted) throw task.Exception!;
	}

	/// <summary>
	/// Wait for a Task object to complete, be cancelled, or be faulted.
	/// </summary>
	/// <param name="task"></param>
	/// <param name="sleepMs"></param>
	/// <returns></returns>
	/// <exception cref="AggregateException"></exception>
	// ReSharper disable once UnusedMember.Global
	public static T WaitForTask<T>(Task<T> task, int sleepMs = 25)
	{
		while (task.IsCompleted == false && task.IsCanceled == false && task.IsFaulted == false)
		{
			Thread.Sleep(sleepMs);
		}

		if (task.IsFaulted) throw task.Exception!;

		return (task.Result ?? default)!;
	}

	private static readonly TaskFactory TaskFactory = new
		TaskFactory(CancellationToken.None,
			TaskCreationOptions.None,
			TaskContinuationOptions.None,
			TaskScheduler.Default);
	
	/// <summary>
	/// Executes an async Task method which has a void return value synchronously
	/// USAGE: Async.RunSync(() => AsyncMethod());
	/// </summary>
	/// <param name="task">Task method to execute</param>
	public static void RunSync(Func<Task> task)
		=> TaskFactory
			.StartNew(task)
			.Unwrap()
			.GetAwaiter()
			.GetResult();

	/// <summary>
	/// <![CDATA[
	/// Executes an async Task<T> method which has a T return type synchronously
	/// USAGE: T result = Async.RunSync<T>(() => AsyncMethod<T>());
	/// ]]>
	/// </summary>
	/// <typeparam name="TResult">Return Type</typeparam>
	/// <param name="task"><![CDATA[ Task<T> method to execute ]]></param>
	/// <returns></returns>
	public static TResult RunSync<TResult>(Func<Task<TResult>> task)
		=> TaskFactory
			.StartNew(task)
			.Unwrap()
			.GetAwaiter()
			.GetResult();
}
