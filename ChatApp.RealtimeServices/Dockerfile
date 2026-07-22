ARG FINAL_BASE_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src

COPY ["ChatApp.RealtimeServices.slnx", "."]
COPY ["ChatApp.Realtime.Abstractions/ChatApp.Realtime.Abstractions.csproj", "ChatApp.Realtime.Abstractions/"]
COPY ["ChatApp.Realtime.Infrastructure.Core/ChatApp.Realtime.Infrastructure.Core.csproj", "ChatApp.Realtime.Infrastructure.Core/"]
COPY ["ChatApp.Realtime.Infrastructure.Nats/ChatApp.Realtime.Infrastructure.Nats.csproj", "ChatApp.Realtime.Infrastructure.Nats/"]
COPY ["ChatApp.Realtime.Infrastructure.Postgres/ChatApp.Realtime.Infrastructure.Postgres.csproj", "ChatApp.Realtime.Infrastructure.Postgres/"]
COPY ["ChatApp.Realtime.Infrastructure.Redis/ChatApp.Realtime.Infrastructure.Redis.csproj", "ChatApp.Realtime.Infrastructure.Redis/"]
COPY ["ChatApp.RealtimeServices/ChatApp.RealtimeServices.csproj", "ChatApp.RealtimeServices/"]
RUN dotnet restore "ChatApp.RealtimeServices/ChatApp.RealtimeServices.csproj"

COPY . .
RUN dotnet publish "ChatApp.RealtimeServices/ChatApp.RealtimeServices.csproj" \
    --configuration $BUILD_CONFIGURATION \
    --no-restore \
    --output /app/publish \
    /p:UseAppHost=false

FROM ${FINAL_BASE_IMAGE} AS final
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER $APP_UID
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=3 \
    CMD ["dotnet", "ChatApp.RealtimeServices.dll", "--health-check", "http://127.0.0.1:8080/ready"]
ENTRYPOINT ["dotnet", "ChatApp.RealtimeServices.dll"]
