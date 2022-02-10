# The function will retrieve CSV files from the Zenodo server, wrangle the data and push the result to the database

using DataFrames, HTTP, CSV, Geodesy

function retrieve(record, files)
    @info(string("Downloading ", chunk, " from Zenodo record #", recordNumber, "..."))

    try
        response = DataFrame(
            CSV.File(
                HTTP.get(string("https://zenodo.org/record/", recordNumber, "/files/", chunk, "?download=1")).body
            )
        )
    catch
        @warn("Download was not successful")
    end

    @info(string("The server returned ", nrow(response), " rows of data. Wrangling..."))

    response = rename(response,
        :latitude_1 => :firstlatitude,
        :longitude_1 => :firstlongitude,
        :altitude_1 => :firstaltitude,
        :latitude_2 => :lastlatitude,
        :longitude_2 => :lastlongitude,
        :altitude_2 => :lastaltitude
    )

    @info("Parsing dates...")
    transform!(
        response,
        [:firstseen, :lastseen] .=> ByRow(dateString -> DateTime(dateString, "yyyy-mm-dd HH:MM:SS+00:00")) .=> [:firstseen, :lastseen],
        :day => ByRow(dateString -> Date(dateString, "yyyy-mm-dd HH:MM:SS+00:00")) => :day
    )

    transform!(
        response,
        [:firstseen, :lastseen] => ByRow((from, until) -> round(Dates.value(until - from) / 60000)) => :duration
    )

    @info("Calculating distances and durations...")
    transform!(
        response, [
            :firstlatitude,
            :firstlongitude,
            :lastlatitude,
            :lastlongitude] => ByRow(
            passmissing(
                (lat1, lon1, lat2, lon2) -> round(haversine((lat1, lon1), (lat2, lon2), 6371))
            )
        ) => :distance
    )

end