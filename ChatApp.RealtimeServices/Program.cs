using ChatApp.RealtimeServices.DependencyInjection;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

try
{
    var builder = Host.CreateApplicationBuilder(args);

    builder.Logging.ClearProviders();
    builder.Logging.AddSimpleConsole(options =>
    {
        options.SingleLine = true;
        options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
    });

    builder.Services.AddRealtimeServices(builder.Configuration);

    using var host = builder.Build();

    var realtimeOptions = host.Services.GetRequiredService<IOptions<RealtimeOptions>>().Value;
    var logger = host.Services.GetRequiredService<ILoggerFactory>()
        .CreateLogger("ChatApp.RealtimeServices");

    logger.LogInformation(
        "正在启动实时服务。服务名={ServiceName}；实例={InstanceId}；环境={Environment}",
        realtimeOptions.ServiceName,
        realtimeOptions.InstanceId,
        host.Services.GetRequiredService<IHostEnvironment>().EnvironmentName);

    await host.RunAsync().ConfigureAwait(false);
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"ChatApp.RealtimeServices 启动失败：{ex.Message}");
    Console.Error.WriteLine(ex);
    return 1;
}
