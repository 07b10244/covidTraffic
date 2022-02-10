using DataFrames, HTTP, Distances, CSV, GZip, Distances, Dates

file = GZip.open("flightlist_20200501_20200531.csv.gz")


response = DataFrame(CSV.File(file))

response = rename(
    response,
    :latitude_1 => :firstlatitude,
    :longitude_1 => :firstlongitude,
    :altitude_1 => :firstaltitude,
    :latitude_2 => :lastlatitude,
    :longitude_2 => :lastlongitude,
    :altitude_2 => :lastaltitude
)

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

transform!(
    response,
    [:firstseen, :lastseen] .=> ByRow(dateString -> DateTime(dateString, "yyyy-mm-dd HH:MM:SS+00:00")) .=> [:firstseen, :lastseen],
    :day => ByRow(dateString -> Date(dateString, "yyyy-mm-dd HH:MM:SS+00:00")) => :day
)

transform!(
    response,
    [:firstseen, :lastseen] => ByRow((from, until) -> round(Dates.value(until - from) / 60000)) => :duration
)

