{{ config 
    (
        alias = 'seasonal_metrics_agg'
    )
}}

with src1 as (select * from {{ ref('games') }}), 
    src2 as (select * from {{ ref('teams') }})

(
    select 
        GAME_DATE,
        GAME_TIME,
        VISITOR,
        VISITOR_GOALS,
        HOME,
        HOME_GOALS,
        GUESTS_IN_ATTENDANCE,
        LENGTH_OF_GAME,
        GP,
        OVERALL_WINS,
        OVERALL_LOSSES,
        OVERTIME_LOSSES,
        TOTAL_POINTS,
        POINTS_PERCENTAGE,
        GOALS_FOR,
        GOALS_AGAINST,
        HOCKEY_REFERENCE_SRS,
        STRENGTH_OF_SCHEDULE,
        POINTS_PERCENTAGE_IN_REGULATION,
        WINS_IN_REGULATION,
        REGULATION_RECORD
    from src1 
    join src2 
        on src1.visitor = src2.team
)
union 
(
    select 
        GAME_DATE,
        GAME_TIME,
        VISITOR,
        VISITOR_GOALS,
        HOME,
        HOME_GOALS,
        GUESTS_IN_ATTENDANCE,
        LENGTH_OF_GAME,
        GP,
        OVERALL_WINS,
        OVERALL_LOSSES,
        OVERTIME_LOSSES,
        TOTAL_POINTS,
        POINTS_PERCENTAGE,
        GOALS_FOR,
        GOALS_AGAINST,
        HOCKEY_REFERENCE_SRS,
        STRENGTH_OF_SCHEDULE,
        POINTS_PERCENTAGE_IN_REGULATION,
        WINS_IN_REGULATION,
        REGULATION_RECORD
    from src1 
    join src2 
    on src1.home = src2.team
)