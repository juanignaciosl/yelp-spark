package com.juanignaciosl

package object yelp {
  // Type aliasing makes code much more readable and doesn't incurs into performance penalty.
  // This doesn't provide type safety, though. If that was preferred, value classes could
  // be used. We chose a simpler approach.
  type BusinessId = String
  type PostalCode = String
  type City = String
  type StateAbbr = String

  type BusinessSchedule = String
}
