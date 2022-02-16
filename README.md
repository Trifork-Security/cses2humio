# CrowdStrike Falcon Event Stream to Humio

This project intend to provide a simple way of moving your CrowdStrike Falcon Event Stream data into Humio.  
As is the only reliable way of getting Event Stream data is with CrowdStrike's SIEM connector, that dumps to files.  
We're trying to bypass the file stage and ship this directly to Humio and streamline ingest, providing CrowdStrike customers  
with Humio a simple way to maintain, visualize and alert on Falcon Event Stream data.

## Design

This project is build as a Python package (application) to be shipped within a Docker or other containerized environment.
The applications error handling could be better, and the primary way to respond to unexpected errors is to shutdown, relying on docker to restart the process.

## Installation

I will fill this out later

## Building

I will fill this out later

## Contributing

I will fill this out later
