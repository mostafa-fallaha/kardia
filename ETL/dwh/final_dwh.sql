CREATE TABLE `f_data` (
  `id` int PRIMARY KEY,
  `survey_date` date,
  `diagnose_date` date,
  `state_code` int,
  `gender_id` int,
  `general_health_id` int,
  `physical_health_days` int,
  `mental_health_days` int,
  `last_checkup_time_id` int,
  `physical_activities` int,
  `sleep_hours` int,
  `removed_teeth_id` int,
  `had_heart_attack` int,
  `had_angina` int,
  `had_stroke` int,
  `had_asthma` int,
  `had_skin_cancer` int,
  `had_copd` int,
  `had_depressive_disorder` int,
  `had_kidney_disease` int,
  `had_arthritis` int,
  `diabetes_status_id` int,
  `deaf_or_hard_of_hearing` int,
  `blind_or_vision_difficulity` int,
  `difficulty_concentrating` int,
  `difficulty_walking` int,
  `difficulty_dressing_bathing` int,
  `difficulty_errands` int,
  `smoking_status_id` int,
  `e_cigarette_usage_id` int,
  `chest_scan` int,
  `race_ethnicity_category_id` int,
  `age_category_id` int,
  `height_in_meters` float,
  `weight_in_kilograms` float,
  `bmi` float,
  `alcohol_drinkers` int,
  `hiv_testing` int,
  `flu_vax_last_12` int,
  `pneumo_vax_ever` int,
  `tetanus_last_10_tdap_id` int,
  `high_risk_last_year` int,
  `covid_pos_id` int
);

CREATE TABLE `d_state` (
  `code` int PRIMARY KEY,
  `state` varchar(255)
);

CREATE TABLE `d_gender` (
  `id` int PRIMARY KEY,
  `gender` varchar(255)
);

CREATE TABLE `d_general_health` (
  `id` int PRIMARY KEY,
  `general_health` varchar(255)
);

CREATE TABLE `d_last_checkup_time` (
  `id` int PRIMARY KEY,
  `last_checkup_time` varchar(255)
);

CREATE TABLE `d_removed_teeth` (
  `id` int PRIMARY KEY,
  `removed_teeth` varchar(255)
);

CREATE TABLE `d_diabetes_status` (
  `id` int PRIMARY KEY,
  `diabetes_status` varchar(255)
);

CREATE TABLE `d_smoking_status` (
  `id` int PRIMARY KEY,
  `smoking_status` varchar(255)
);

CREATE TABLE `d_e_cigarette_usage` (
  `id` int PRIMARY KEY,
  `e_cigarette_usage` varchar(255)
);

CREATE TABLE `d_race_ethnicity_category` (
  `id` int PRIMARY KEY,
  `race_ethnicity_category` varchar(255)
);

CREATE TABLE `d_age_category` (
  `id` int PRIMARY KEY,
  `age_category` varchar(255)
);

CREATE TABLE `d_tetanus_last_10_tdap` (
  `id` int PRIMARY KEY,
  `tetanus_last_10_tdap` varchar(255)
);

CREATE TABLE `d_covid_pos` (
  `id` int PRIMARY KEY,
  `covid_pos` varchar(255)
);

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_state`
FOREIGN KEY (`state_code`) REFERENCES `d_state` (`code`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_gender`
FOREIGN KEY (`gender_id`) REFERENCES `d_gender` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_gen_heatlh`
FOREIGN KEY (`general_health_id`) REFERENCES `d_general_health` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_last_checkup`
FOREIGN KEY (`last_checkup_time_id`) REFERENCES `d_last_checkup_time` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_removed_teeth`
FOREIGN KEY (`removed_teeth_id`) REFERENCES `d_removed_teeth` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_diabetes`
FOREIGN KEY (`diabetes_status_id`) REFERENCES `d_diabetes_status` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_smoking_status`
FOREIGN KEY (`smoking_status_id`) REFERENCES `d_smoking_status` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_e_cigarette`
FOREIGN KEY (`e_cigarette_usage_id`) REFERENCES `d_e_cigarette_usage` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_race`
FOREIGN KEY (`race_ethnicity_category_id`) REFERENCES `d_race_ethnicity_category` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_age_category`
FOREIGN KEY (`age_category_id`) REFERENCES `d_age_category` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_tetanus`
FOREIGN KEY (`tetanus_last_10_tdap_id`) REFERENCES `d_tetanus_last_10_tdap` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `f_data` ADD CONSTRAINT `fk_data_covid_pos`
FOREIGN KEY (`covid_pos_id`) REFERENCES `d_covid_pos` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;
