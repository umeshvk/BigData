package com.mvdb.etl.data;

import java.util.Date;

public interface MvdbUpdateTimeMaker
{
    Date makeMvdbUpdateTime(Object originalUpdateTimeValue);
}
