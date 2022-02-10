# Updates the database with the latest Opensky-Datasets, wrangles data and pushes result to local database

include("functions/retrieveData.jl")

filesList = [
    "flightlist_20190101_20190131.csv.gz",
    "flightlist_20190201_20190228.csv.gz"
]

for file in filesList
    @info(file)
end


