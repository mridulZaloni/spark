package com.zaloni.mgohain.learnspark.secondary_sorting

/**
  * Created by mgohain on 3/4/2016.
  */
/**
  * Key for the secondary sort
  *
  * @param flightDate date of the flight
  * @param carrier carrier identifier
  * @param destAirportId destination airport id
  * @param delay flight delay time
  */
case class FlightKey(flightDate: String, carrier: String, destAirportId : String, delay : String)
object FlightKey {
  implicit def orderingByDateIdDelay[A <: FlightKey] : Ordering[A] = {
    Ordering.by(fk => (fk.carrier, fk.destAirportId, fk.delay * -1))
  }
}
