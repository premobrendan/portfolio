from soria_api.diglet.types import BaseChildScraper


class CDCAPIChildScraper(BaseChildScraper):
    scraper_id: str
    name: str
    title: str
    url: str
    resource_id: str


CDC_API_CHILD_SCRAPERS = {
    "CDC_98_weekly_hospital_respiratory_data_prelim": CDCAPIChildScraper(
        scraper_id="CDC_98",
        name="CDC_98_weekly_hospital_respiratory_data_prelim",
        title="Weekly Hospital Respiratory Data (HRD) Metrics by Jurisdiction, National Healthcare Safety Network (NHSN) (Preliminary)",
        url="https://data.cdc.gov/resource/mpgq-jmmr.csv",
        resource_id="mpgq-jmmr"
    ),
    "CDC_99_weekly_hospital_respiratory_data": CDCAPIChildScraper(
        scraper_id="CDC_99",
        name="CDC_99_weekly_hospital_respiratory_data",
        title="Weekly Hospital Respiratory Data (HRD) Metrics by Jurisdiction, National Healthcare Safety Network (NHSN)",
        url="https://data.cdc.gov/resource/ua7e-t2fy.csv",
        resource_id="ua7e-t2fy"
    ),
    "CDC_114_health_insurance_coverage": CDCAPIChildScraper(
        scraper_id="CDC_114",
        name="CDC_114_health_insurance_coverage",
        title="Indicators of Health Insurance Coverage at the Time of Interview",
        url="https://data.cdc.gov/resource/jb9g-gnvr.csv",
        resource_id="jb9g-gnvr"
    ),
    "CDC_115_estimated_annual_visits_to_health_centers": CDCAPIChildScraper(
        scraper_id="CDC_115",
        name="CDC_115_estimated_annual_visits_to_health_centers",
        title="Estimated annual visits to health centers",
        url="https://data.cdc.gov/resource/367e-pucc.csv",
        resource_id="367e-pucc"
    ),
    "CDC_100_births_fertility_rates": CDCAPIChildScraper(
        scraper_id="CDC_100",
        name="CDC_100_births_fertility_rates",
        title="NCHS - Births and General Fertility Rates: United States",
        url="https://data.cdc.gov/resource/e6fc-ccez.csv",
        resource_id="e6fc-ccez"
    ),
    "CDC_101_covid_vax_pharmacies": CDCAPIChildScraper(
        scraper_id="CDC_101",
        name="CDC_101_covid_vax_pharmacies",
        title="Weekly Cumulative Estimated Number of COVID-19 Vaccinations Administered in Pharmacies and Physician Medical Offices, Adults 18 years and older, by Season and Age Group, United States",
        url="https://data.cdc.gov/resource/ewpg-rz7g.csv",
        resource_id="ewpg-rz7g"
    ),
    "CDC_103_laboratory_confirmed_rsv_covid19_flu_hospitalizations": CDCAPIChildScraper(
        scraper_id="CDC_103",
        name="CDC_103_laboratory_confirmed_rsv_covid19_flu_hospitalizations",
        title="Rates of Laboratory-Confirmed RSV, COVID-19, and Flu Hospitalizations from the RESP-NET Surveillance Systems",
        url="https://data.cdc.gov/resource/kvib-3txy.csv",
        resource_id="kvib-3txy"
    ),
    "CDC_104_emergency_department_visit_trajectories": CDCAPIChildScraper(
        scraper_id="CDC_104",
        name="CDC_104_emergency_department_visit_trajectories",
        title="NSSP Emergency Department Visit Trajectories by State and Sub State Regions- COVID-19, Flu, RSV, Combined",
        url="https://data.cdc.gov/resource/rdmq-nq56.csv",
        resource_id="rdmq-nq56"
    ),
    "CDC_106_level_of_acute_respiratory_illness": CDCAPIChildScraper(
        scraper_id="CDC_106",
        name="CDC_106_level_of_acute_respiratory_illness",
        title="Level of Acute Respiratory Illness (ARI) Activity by State",
        url="https://data.cdc.gov/resource/f3zz-zga5.csv",
        resource_id="f3zz-zga5"
    ),
    "CDC_107_drug_use_data_from_selected_hospitals": CDCAPIChildScraper(
        scraper_id="CDC_107",
        name="CDC_107_drug_use_data_from_selected_hospitals",
        title="Drug Use Data from Selected Hospitals",
        url="https://data.cdc.gov/resource/gypc-kpgn.csv",
        resource_id="gypc-kpgn"
    ),
    "CDC_108_dentists_by_state": CDCAPIChildScraper(
        scraper_id="CDC_108",
        name="CDC_108_dentists_by_state",
        title="DQS Dentists, by state: United States.",
        url="https://data.cdc.gov/resource/9epi-jrff.csv",
        resource_id="9epi-jrff"
    ),
    "CDC_109_community_hospital_beds_by_state": CDCAPIChildScraper(
        scraper_id="CDC_109",
        name="CDC_109_community_hospital_beds_by_state",
        title="DQS Community hospital beds by state",
        url="https://data.cdc.gov/resource/uiux-mrvg.csv",
        resource_id="uiux-mrvg"
    ),
    "CDC_117_medicaid_coverage_among_persons_under_age_65": CDCAPIChildScraper(
        scraper_id="CDC_117",
        name="CDC_117_medicaid_coverage_among_persons_under_age_65",
        title="DQS Medicaid coverage among persons under age 65, by selected characteristics: United States",
        url="https://data.cdc.gov/resource/hdja-ybdg.csv",
        resource_id="hdja-ybdg"
    ),
    "CDC_120_vsrr_provisional_estimates_for_indicators_of_mortality": CDCAPIChildScraper(
        scraper_id="CDC_120",
        name="CDC_120_vsrr_provisional_estimates_for_indicators_of_mortality",
        title="NCHS - VSRR Quarterly provisional estimates for selected indicators of mortality",
        url="https://data.cdc.gov/resource/489q-934x.csv",
        resource_id="489q-934x"
    ),
    "CDC_105_respiratory_virus_response_nssp_emergency_department_visits_covid19_flu_rsv_combined": CDCAPIChildScraper(
        scraper_id="CDC_105",
        name="CDC_105_respiratory_virus_response_nssp_emergency_department_visits_covid19_flu_rsv_combined",
        title="2023 Respiratory Virus Response - NSSP Emergency Department Visits - COVID-19, Flu, RSV, Combined",
        url="https://data.cdc.gov/resource/vutn-jzwm.csv",
        resource_id="vutn-jzwm"
    ),
    "CDC_110_health_care_employment_and_wages": CDCAPIChildScraper(
        scraper_id="CDC_110",
        name="CDC_110_health_care_employment_and_wages",
        title="DQS Health care employment and wages, by selected occupations: United States",
        url="https://data.cdc.gov/resource/q9cb-be9u.csv",
        resource_id="q9cb-be9u"
    ),
    "CDC_111_hospital_admission_average_length_of_stay_outpatient_visits_and_outpatient_surgery": CDCAPIChildScraper(
        scraper_id="CDC_111",
        name="CDC_111_hospital_admission_average_length_of_stay_outpatient_visits_and_outpatient_surgery",
        title="DQS Hospital admission, average length of stay, outpatient visits, and outpatient surgery by type of ownership and size of hospital: United States",
        url="https://data.cdc.gov/resource/rear-2epk.csv",
        resource_id="rear-2epk"
    ),
    "CDC_113_prescription_drug_use_in_the_past_30_days": CDCAPIChildScraper(
        scraper_id="CDC_113",
        name="CDC_113_prescription_drug_use_in_the_past_30_days",
        title="DQS Prescription drug use in the past 30 days, by sex, race and Hispanic origin, and age group: United States",
        url="https://data.cdc.gov/resource/b666-c5v5.csv",
        resource_id="b666-c5v5"
    ),
    "CDC_118_estimates_of_emergency_department_visits": CDCAPIChildScraper(
        scraper_id="CDC_118",
        name="CDC_118_estimates_of_emergency_department_visits",
        title="Estimates of Emergency Department Visits in the United States from 2016-2022",
        url="https://data.cdc.gov/resource/ycxr-emue.csv",
        resource_id="ycxr-emue"
    ),
    "CDC_112_estimates_of_emergency_department_visits_footnotes": CDCAPIChildScraper(
        scraper_id="CDC_112",
        name="CDC_112_estimates_of_emergency_department_visits_footnotes",
        title="DQS Estimate of Emergency Department Visits in the United States Footnotes",
        url="https://data.cdc.gov/resource/6vwk-ensg.csv",
        resource_id="6vwk-ensg"
    ),
    "CDC_116_visits_to_physician_offices_outpatient_departments_emergency_departments": CDCAPIChildScraper(
        scraper_id="CDC_116",
        name="CDC_116_visits_to_physician_offices_outpatient_departments_emergency_departments",
        title="DQS Visits to physician offices, hospital outpatient departments, and hospital emergency departments, by age, sex, and race: United States",
        url="https://data.cdc.gov/resource/xmjk-wh9b.csv",
        resource_id="xmjk-wh9b"
    )
}
