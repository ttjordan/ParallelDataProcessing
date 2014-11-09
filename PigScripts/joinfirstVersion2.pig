REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
SET default_parallel 10;

Flights1 = LOAD '$INPUT' USING CSVLoader AS (Year, Quarter, Month, DayofMonth, DayOfWeek, FlightDate, UniqueCarrier, AirlineID, Carrier, TailNum, FlightNum, Origin, OriginCityName, OriginState, OriginStateFips, OriginStateName, OriginWac, Dest, DestCityName, DestState, DestStateFips, DestStateName, DestWac, CRSDepTime, DepTime, DepDelay, DepDelayMinutes, DepDel15, DepartureDelayGroups, DepTimeBlk, TaxiOut, WheelsOff, WheelsOn, TaxiIn, CRSArrTime, ArrTime, ArrDelay, ArrDelayMinutes, ArrDel15, ArrivalDelayGroups, ArrTimeBlk, Cancelled, CancellationCode, Diverted, CRSElapsedTime, ActualElapsedTime, AirTime, Flights, Distance, DistanceGroup, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay);

Flights2 = LOAD '$INPUT' USING CSVLoader AS (Year, Quarter, Month, DayofMonth, DayOfWeek, FlightDate, UniqueCarrier, AirlineID, Carrier, TailNum, FlightNum, Origin, OriginCityName, OriginState, OriginStateFips, OriginStateName, OriginWac, Dest, DestCityName, DestState, DestStateFips, DestStateName, DestWac, CRSDepTime, DepTime, DepDelay, DepDelayMinutes, DepDel15, DepartureDelayGroups, DepTimeBlk, TaxiOut, WheelsOff, WheelsOn, TaxiIn, CRSArrTime, ArrTime, ArrDelay, ArrDelayMinutes, ArrDel15, ArrivalDelayGroups, ArrTimeBlk, Cancelled, CancellationCode, Diverted, CRSElapsedTime, ActualElapsedTime, AirTime, Flights, Distance, DistanceGroup, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay);

Flights1_filtered = FILTER Flights1 BY (Origin == 'ORD') AND (ArrDelayMinutes is not null) AND (ArrTime is not null) AND (Cancelled is not null) AND (Cancelled neq '1') AND (DepTime is not null) AND (Dest is not null) AND (Diverted is not null) AND (Diverted neq '1') AND (FlightDate is not null) AND (Origin is not null);

Flights2_filtered = FILTER Flights2 BY (Dest == 'JFK') AND (ArrDelayMinutes is not null) AND (ArrTime is not null) AND (Cancelled is not null) AND (Cancelled neq '1') AND (DepTime is not null) AND (Dest is not null) AND (Diverted is not null) AND (Diverted neq '1') AND (FlightDate is not null) AND (Origin is not null);

join_result = JOIN Flights1_filtered BY (Dest, FlightDate), Flights2_filtered BY (Origin, FlightDate);

filtered_join_result = FILTER join_result by ((Flights1_filtered::Year == 2007 AND Flights1_filtered::Month >= 6) OR (Flights1_filtered::Year == 2008 AND Flights1_filtered::Month <= 5)) AND (Flights1_filtered::ArrTime < Flights2_filtered::DepTime);

sum_delays = FOREACH filtered_join_result GENERATE (Flights1_filtered::ArrDelayMinutes + Flights2_filtered::ArrDelayMinutes);
sum_delays_group = GROUP sum_delays all;

avg = FOREACH sum_delays_group generate AVG(sum_delays);

STORE avg into '$OUTPUT';



