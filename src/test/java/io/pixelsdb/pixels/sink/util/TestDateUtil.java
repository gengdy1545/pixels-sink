/*
 * Copyright 2026 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
