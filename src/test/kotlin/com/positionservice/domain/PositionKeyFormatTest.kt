package com.positionservice.domain

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PositionKeyFormatTest {

    @Test
    fun `BOOK_COUNTERPARTY_INSTRUMENT generates correct key`() {
        val key = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT.generateKey("BOOK1", "GOLDMAN", "AAPL")
        assertEquals("BOOK1#GOLDMAN#AAPL", key)
    }

    @Test
    fun `BOOK_INSTRUMENT generates correct key`() {
        val key = PositionKeyFormat.BOOK_INSTRUMENT.generateKey("BOOK1", "GOLDMAN", "AAPL")
        assertEquals("BOOK1#AAPL", key)
    }

    @Test
    fun `COUNTERPARTY_INSTRUMENT generates correct key`() {
        val key = PositionKeyFormat.COUNTERPARTY_INSTRUMENT.generateKey("BOOK1", "GOLDMAN", "AAPL")
        assertEquals("GOLDMAN#AAPL", key)
    }

    @Test
    fun `INSTRUMENT generates correct key`() {
        val key = PositionKeyFormat.INSTRUMENT.generateKey("BOOK1", "GOLDMAN", "AAPL")
        assertEquals("AAPL", key)
    }

    @Test
    fun `BOOK generates correct key`() {
        val key = PositionKeyFormat.BOOK.generateKey("BOOK1", "GOLDMAN", "AAPL")
        assertEquals("BOOK1", key)
    }
}
