--Verify if data is inserted correctly based on count of records
select count(*) from events;

--Add event_week column for getting results
alter table events add event_week int;

--Populate the event_week column with the number of week based on timestamp
update events set event_week = week(CAST(`timestamp` as date))


select
    event_week,
    sec_to_time(
        sum(event_duration) / count(distinct global_session_id)
    ) as avg_user_session_week,
    count(distinct global_session_id) as total_sessions_week
FROM
/*Calculating session duration*/
    (
        select
            *,
            sum(event_duration) OVER (PARTITION BY global_session_id) as session_duration
        FROM
        /*Calculating global unique session, unique session for each user within a global session and total sessions in a week*/
            (
                select
                    *,
                    sum(is_new_session) OVER (
                        ORDER by
                            user_id,
                            `timestamp`
                    ) as global_session_id,
                    sum(is_new_session) OVER (
                        PARTITION by user_id
                        order by
                            `timestamp`
                    ) as user_session_id,
                    sum(is_new_session) over (PARTITION by event_week) as total_sessions
                from
                /*Defining a new session based on the 5 minute condition and calculating the time difference between each event*/
                    (
                        select
                            *,
                            timestampdiff(SECOND, last_event, `timestamp`) as event_duration,
                            case
                                when timestampdiff(SECOND, last_event, `timestamp`) >= (5 * 60)
                                or last_event is NULL then 1
                                else 0
                            end as is_new_session
                        from
                        /*Query to record last event in a new column*/
                            (
                                select
                                    *,
                                    LAG(`timestamp`, 1) OVER (
                                        PARTITION BY user_id
                                        ORDER BY
                                            `timestamp`
                                    ) AS last_event
                                from
                                    events
                                /*Exlcuding Email and push events*/
                                WHERE
                                    event not like '%email%'
                                order by
                                    user_id,
                                    `timestamp`
                            ) LAST
                    ) FINAL
            ) FINAL
        /*Excluding beginning of new session to exclude time added since last event*/
        where
            is_new_session = 0
    ) FINAL
/*Excluding sessions under 2 minutes*/
where
    session_duration >= 120
group by
    event_week;



/*  ***OBSERVATION***  */
/*

Some sessions that continue and overlap in 2 continuous weeks would get added to the total sessions of both weeks
thus adding an extra session in the next week

*/
