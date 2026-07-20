/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the license, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.util;

import io.pixelsdb.pixels.core.utils.DatetimeUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;

public final class TestDateUtil
{
    private TestDateUtil()
    {
    }

    public static Date fromDebeziumDate(int epochDay)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(1970, Calendar.JANUARY, 1);
        calendar.add(Calendar.DAY_OF_MONTH, epochDay);
        return calendar.getTime();
    }

    public static String convertDateToDayString(Date date)
    {
        return new java.text.SimpleDateFormat("yyyy-MM-dd").format(date);
    }

    public static String convertDebeziumTimestampToString(long epochTs)
    {
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(epochTs), ZoneId.systemDefault());
        return dateTime.format(DatetimeUtils.SQL_LOCAL_DATE_TIME);
    }
}
