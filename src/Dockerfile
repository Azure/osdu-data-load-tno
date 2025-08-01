# Use the official Microsoft .NET 9.0 SDK image with Python support
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build

# Install Python 3
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy solution and project files
COPY *.sln ./
COPY OSDU.DataLoad.Domain/*.csproj ./OSDU.DataLoad.Domain/
COPY OSDU.DataLoad.Application/*.csproj ./OSDU.DataLoad.Application/
COPY OSDU.DataLoad.Infrastructure/*.csproj ./OSDU.DataLoad.Infrastructure/
COPY OSDU.DataLoad.Console/*.csproj ./OSDU.DataLoad.Console/

# Create a custom NuGet.config to override any Windows-specific settings
RUN echo '<?xml version="1.0" encoding="utf-8"?>\n\
<configuration>\n\
  <packageSources>\n\
    <clear />\n\
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" protocolVersion="3" />\n\
  </packageSources>\n\
  <fallbackPackageFolders>\n\
    <clear />\n\
  </fallbackPackageFolders>\n\
</configuration>' > NuGet.config

# Clear any existing NuGet configuration and restore dependencies
RUN dotnet nuget locals all --clear
RUN dotnet restore --no-cache --configfile NuGet.config

# Copy the rest of the source code
COPY . ./

# Build the solution directly without clean (to avoid the cached issue)
RUN dotnet build --configuration Release --no-restore --verbosity minimal

# Publish the console application
RUN dotnet publish OSDU.DataLoad.Console/OSDU.DataLoad.Console.csproj \
    --configuration Release \
    --no-build \
    --output /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime

# Install Python 3 in the runtime image
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Create symlink for python command
RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

# Copy the published application
COPY --from=build /app/publish .

# Copy generate-manifest-scripts directory to the parent directory level (same as local development)
COPY --from=build /app/generate-manifest-scripts /generate-manifest-scripts/

# Set the entry point
ENTRYPOINT ["dotnet", "OSDU.DataLoad.Console.dll"]
