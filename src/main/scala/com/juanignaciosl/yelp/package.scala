package com.juanignaciosl

package object yelp {
  // Type aliasing makes code much more readable and doesn't incurs into performance penalty.
  // This doesn't provide type safety, though. If that was preferred, value classes could
  // be used. We chose a simpler approach.
  type BusinessId = String
  type PostalCode = String
  type City = String
  type StateAbbr = String

  /**
   * Example: 10:00-21:00.
   */
  type BusinessSchedule = String

  /**
   * Example: 10:00.
   */
  type BusinessTime = String

  type BusinessGrouping = (StateAbbr, City, PostalCode)

  type BusinessCityGrouping = (StateAbbr, City)

  type SevenLongs = (Long, Long, Long, Long, Long, Long, Long)

  type ReviewId = String

  type CoolnessCount = Long
}
