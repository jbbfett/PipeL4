FROM ubuntu:24.04 AS build

# Ubuntu updates the package revision; global.json constrains SDK selection to the
# requested version or its supported patch roll-forward within the same feature band.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        dotnet-sdk-10.0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY global.json ./
COPY .config/dotnet-tools.json .config/dotnet-tools.json
RUN dotnet --version \
    && dotnet tool restore

COPY . .

RUN dotnet paket restore --project src/PipeL4/PipeL4.fsproj \
    && dotnet restore src/PipeL4/PipeL4.fsproj /p:PaketDisableGlobalRestore=true

RUN dotnet publish src/PipeL4/PipeL4.fsproj \
    --configuration Release \
    --no-restore \
    --output /app/publish \
    /p:UseAppHost=false \
    /p:PaketRestoreDisabled=true

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final

WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "PipeL4.dll"]
