FROM ubuntu:24.04 AS build

ARG DOTNET_SDK_PACKAGE_VERSION=10.0.112-0ubuntu1~24.04.1
ARG PAKET_VERSION=10.3.1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        dotnet-sdk-10.0=${DOTNET_SDK_PACKAGE_VERSION} \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

RUN dotnet tool install --tool-path /tools Paket --version ${PAKET_VERSION}
ENV PATH="/tools:${PATH}"

COPY . .

RUN paket restore --project src/PipeL4/PipeL4.fsproj \
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
