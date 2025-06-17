DROP TABLE IF EXISTS transactions_v2;

CREATE TABLE transactions_v2 (
    id Uint64 NOT NULL,
    age Uint8,
    gender Utf8,
    job_type Utf8,
    daily_social_media_time Double,
    social_platform_preference Utf8,
    number_of_notifications Uint16,
    work_hours_per_day Double,
    perceived_productivity_score Double,
    actual_productivity_score Double,
    stress_level Uint8,
    sleep_hours Double,
    screen_time_before_sleep Double,
    breaks_during_work Uint8,
    uses_focus_apps Bool,
    has_digital_wellbeing_enabled Bool,
    coffee_consumption_per_day Uint8,
    days_feeling_burnout_per_month Uint8,
    weekly_offline_hours Double,
    job_satisfaction_score Double,
    PRIMARY KEY (id)
);
