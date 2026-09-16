package org.johnpaulkh.kafkamqtt.utils

import org.jeasy.random.EasyRandom
import org.jeasy.random.EasyRandomParameters
import org.jeasy.random.FieldPredicates
import kotlin.random.Random

val parameters: EasyRandomParameters =
    EasyRandomParameters()
        .randomize(Number::class.java) { Random.nextInt(1_000, 10_000) }
        .randomize(FieldPredicates.named("numberOfDependents")) { Random.nextInt(0, 10) }

val random: EasyRandom = EasyRandom(parameters)

inline fun <reified T> EasyRandom.next(): T = nextObject(T::class.java)
