from core_data_modules.cleaners import swahili
from dateutil.parser import isoparse

from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="engagement-db-test",
    # TODO: store in messages and individuals_filter list of functions.
    project_start_date=isoparse("2021-03-01T10:30:00+03:00"),
    project_end_date=isoparse("2100-01-01T00:00:00+03:00"),
    test_participant_uuids=[
        "avf-participant-uuid-51c15546-58a0-4ab1-b465-e65b71462a8f"
    ],
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/experimental_test"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_test",
        uuid_prefix="avf-participant-uuid-"
    ),
    operations_dashboard=OperationsDashboardConfiguration(
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
    ),
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/experimental-test-text-it-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("test_pipeline_daniel_activation", "rqa_s01e01", "s01e01"),
                    FlowResultConfiguration("test_pipeline_daniel_demog", "constituency", "location"),
                    FlowResultConfiguration("test_pipeline_daniel_demog", "age", "age"),
                    FlowResultConfiguration("test_pipeline_daniel_demog", "gender", "gender"),
                ]
            )
        )
    ],
    csv_sources=[
        CSVSource(
            "gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/test_recovery.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("s01e01", end_date=isoparse("2021-12-31T24:00:00+03:00")),
                CSVDatasetConfiguration("age", start_date=isoparse("2022-01-01T00:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/test_recovery2.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("s01e01", end_date=isoparse("2021-12-31T24:00:00+03:00")),
                CSVDatasetConfiguration("age", start_date=isoparse("2022-01-01T00:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        )
    ],
    google_form_sources=[
        GoogleFormSource(
            google_form_client=GoogleFormsClientConfiguration(
                credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json"
            ),
            sync_config=GoogleFormToEngagementDBConfiguration(
                form_id="17q1yu1rb-gE9sdXnnRKPIAqGU27-uXm_xGVkfI5rudA",
                participant_id_configuration=ParticipantIdConfiguration(
                    question_title="Kenyan Mobile Number",
                    id_type=GoogleFormParticipantIdTypes.KENYA_MOBILE_NUMBER
                ),
                question_configurations=[
                    # Multiple choice question with other
                    QuestionConfiguration(question_titles=["What is your gender?"], engagement_db_dataset="gender"),

                    # Short answer
                    QuestionConfiguration(question_titles=["What is your age?"], engagement_db_dataset="age"),

                    # Long answer
                    QuestionConfiguration(question_titles=["Test Question 1"], engagement_db_dataset="s01e01"),

                    # Short answer
                    QuestionConfiguration(question_titles=["Test Question 2"], engagement_db_dataset="s01e02"),
                ]
            )
        ),
        GoogleFormSource(
            google_form_client=GoogleFormsClientConfiguration(
                credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json"
            ),
            sync_config=GoogleFormToEngagementDBConfiguration(
                form_id="1cEeq9ujJTv381xTXEB0oP0vLNnSLIfP9Rz32zL1HnHk",
                participant_id_configuration=ParticipantIdConfiguration(
                    question_title="What is your phone number",
                    id_type=GoogleFormParticipantIdTypes.KENYA_MOBILE_NUMBER
                ),
                ignore_invalid_mobile_numbers=True,
                question_configurations=[
                    # Demographic Questions
                    QuestionConfiguration(engagement_db_dataset="aik_language", question_titles=["We could either do the interview in English or Swahili. Which language would you prefer? "]),
                    QuestionConfiguration(engagement_db_dataset="age", question_titles=["Do you mind telling me how old you are?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_education", question_titles=["What is the highest level of education attained ? "]),
                    QuestionConfiguration(engagement_db_dataset="aik_employment_status", question_titles=["What is your employment Status ?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_religion", question_titles=["What is your religion ?"]),
                    QuestionConfiguration(engagement_db_dataset="gender", question_titles=["What is your sex? "]),
                    QuestionConfiguration(engagement_db_dataset="aik_household_income", question_titles=["Approximately what is your gross monthly household income? (I.e. This is the combined monthly income of all your household members). This will help us in determining your social-economic class."]),
                    QuestionConfiguration(engagement_db_dataset="disabled", question_titles=["Do you have any form of disability? (If disability is visible, do not ask, make the judgement)"]),
                    QuestionConfiguration(engagement_db_dataset="aik_communities", question_titles=["What community do you belong to?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_indigenous_or_minority", question_titles=["Is it considered indigenous or minority?  if yes provide details."]),
                    QuestionConfiguration(engagement_db_dataset="location", question_titles=["Can I presume that you are currently a resident of; …………… County? [mention name of the target county]", 
                                                                                                       "Can I presume that you are currently a resident of; …………… Sub-County / Constituency? [mention name of the target sub-county]",
                                                                                                       "Can I presume that you are currently a resident of; …………… Ward? [mention name of the target ward]"]),
                    ## GENERAL ELECTORAL ENVIRONMENT
                    QuestionConfiguration(engagement_db_dataset="aik_voting_participation", question_titles=["Do you plan on voting in the August 9th General Elections?", "If NOT, why are you not planning to vote? "]),
                    QuestionConfiguration(engagement_db_dataset="aik_political_participation", question_titles=["Do you feel comfortable participating in any political activities in your area of residence?", 
                                                                                                               "Reasons for your answer on political activities participation."]),
                    QuestionConfiguration(engagement_db_dataset="aik_political_environment", question_titles=["Do you think the political and security environment is conducive to free and fair elections?"]),

                    ## HATE SPEECH AND INCITEMENT 
                    # QuestionConfiguration(engagement_db_dataset="aik_election_conversations", question_titles=["In your view, have elections-related conversations become more controversial and conflictual in the past two weeks than the two weeks before?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_hate_speech_and_actions_target", question_titles=["Have you heard comments or seen actions motivated by hatred/negative attitudes regarding a person's identity in the last two weeks?", 
                                                                                                                      "If YES, What did they target?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_identity_groups_increase", question_titles=["In your view, has there been an increase in groups with strong political identities challenging others with different loyalties?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_political_events_disruption", question_titles=["In your area, has there been an increase in disruption of political events by the opponent's supporters?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_intolerance_incidents", question_titles=["In your area, has there been an increase in bullying, harassment, and general intolerance incidents? ",
                                                                                                             "If YES, on what grounds?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_unsafe_areas", question_titles=["Are there areas in your community that have become more unsafe in the last two weeks?", 
                                                                                                    "If YES, where and why are they unsafe?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_electoral_violence_anxiety", question_titles=["Are you or your family/neighbours more worried about electoral violence than two weeks ago?", 
                                                                                                                  "If YES, why are they worried about electoral violence?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_electoral_sexual_gender_based_violence", question_titles=["Are you or your family/neighbours more worried about electoral gender-based violence than two weeks ago? ", 
                                                                                                                              "If YES, why are they worried about electoral gender-based violence?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_willingness_to_help_victims", question_titles=["Would you be willing to help a neighbour from a different political view or ethnic background if they were attacked?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_engaging_authorities", question_titles=["Does your household know how to safely and quickly report a crime or seek help from the authorities? "]),
                    QuestionConfiguration(engagement_db_dataset="aik_incitement_sources", question_titles=["Which sources have you seen or heard hateful/inciteful statements about other communities, identities, and religions in the last two weeks?",
                                                                                                          "If there are any, what was the nature of the statements?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_vote_buying_incidents", question_titles=["Have you heard or seen incidents of voters being encouraged not to vote or sell their voters cards?", 
                                                                                                   "If YES, when and where did this incidents of encouragement on not to vote happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_source_of_vote_buying", question_titles=["Who encouraged this?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_influence_on_voting_choices", question_titles=["Based on the campaign activities over the past two weeks, what do you think will influence people's voting choices in your area?"]),
                    
                    ## RISK OF VIOLENCE & CONFLICT
                    QuestionConfiguration(engagement_db_dataset="aik_incidents_of_polarisation", question_titles=["In the last two weeks, have some areas become no go areas for political supporters or ethnic groups.", 
                                                                                                                 "If YES, when and where did this happen, and who is being driven out?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_inability_to_work", question_titles=["Are there community members who have not been able to work in the last two weeks?", 
                                                                                                         "If YES, why has this happened to those community members?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_incidents_of_violence_and_polarisation", question_titles=["Have violent public protests or communal riots taken place?", 
                                                                                                                              "If YES, when, where and why did this public riots or communal riots happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_police_brutality", question_titles=["Have police officers used excessive force and / or live ammunition to respond to protesters?", 
                                                                                                        "If YES, when and where did this incident on police brutality happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_vandalism_theft_incidents", question_titles=["Have people's homes and assets been vandalized and/or stolen? "]),
                    QuestionConfiguration(engagement_db_dataset="aik_physical_harm", question_titles=["Have there been injuries and deaths related to elections activities in the last two weeks?", 
                                                                                                     "If YES, when and where did this election physical harm happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_sexual_assault", question_titles=["Have members of your community been sexually assaulted or raped related to elections activities in the last two weeks?", 
                                                                                                      "If YES, when and where did this sexual assault happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_violence_displacement", question_titles=["Has violence displaced members of your community?", 
                                                                                                             "If YES, when and where did this incidents of displacement happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_concern_about_safety_and_security", question_titles=["Based on the current political and security environment, are you concerned about safety and security within your community?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_peace_and_security_initiatives", question_titles=["Have you heard of Initiatives aimed at enhancing peace and security in the last two weeks?", 
                                                                                                                      "If YES, what were the peace and security initiatives?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_iebc_effectiveness", question_titles=["Independent Electoral and Boundaries Commission (IEBC)", "Add reason for the score on IEBC?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_nps_effectiveness", question_titles=["National Police Service", "Add reason for the score on NPS?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_ncic_effectiveness", question_titles=["National Cohesion and Integration Commission(NCIC)", "Add reason for the score on NCIC?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_dpp_effectiveness", question_titles=["Office of the Director of Public Prosecutions", "Add reason for the score on DPP?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_ipoa_effectiveness", question_titles=["Independent Policing Oversight Authority", "Add reason for the score on IPOA?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_judiciary_effectiveness", question_titles=["The Judiciary", "Add reason for the score on Judiciary?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_knchr_effectiveness", question_titles=["Kenya National Commission on Human Rights", "Add reason for the score on KNCHR?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_other_institutions_effectiveness", question_titles=["List other institutions and their ratings?"])
                ]
            )
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-staging.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_gender",
                    engagement_db_dataset="gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(
                            code_scheme=load_code_scheme("gender"),
                            auto_coder=swahili.DemographicCleaner.clean_gender
                        )
                    ],
                    ws_code_match_value="gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_location",
                    engagement_db_dataset="location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("kenya_constituency"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("kenya_county"), auto_coder=None)
                    ],
                    ws_code_match_value="location"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(
                            code_scheme=load_code_scheme("age"),
                            auto_coder=lambda x: str(swahili.DemographicCleaner.clean_age_within_range(x)),
                            coda_code_schemes_count=2
                        )
                    ],
                    ws_code_match_value="age",
                    dataset_users_file_url=f"gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/TEST_age_coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_s01e01",
                    engagement_db_dataset="s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("s01e01"), auto_coder=None, coda_code_schemes_count=3)
                    ],
                    ws_code_match_value="s01e01"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/coda_users.json"
        )
    ),
    rapid_pro_target=RapidProTarget(
        rapid_pro=RapidProClientConfiguration(
            domain="textit.com",
            token_file_url="gs://avf-credentials/wusc-leap-kalobeyei-textit-token.txt"  #For testing as other workspaces are suspended
        ),
        sync_config=EngagementDBToRapidProConfiguration(
            consent_withdrawn_dataset=DatasetConfiguration(
                engagement_db_datasets=["gender", "location", "age", "s01e01"],
                rapid_pro_contact_field=ContactField(key="engagement_db_consent_withdrawn", label="Engagement DB Consent Withdrawn")
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS,
            # allow_clearing_fields is set somewhat arbitrarily here because this data isn't being used in flows.
            # A pipeline that has continuous sync back in production will need to consider the options carefully.
            allow_clearing_fields=True,
            weekly_advert_contact_field=ContactField(key="test_pipeline_weekly_advert_contacts",
                                                     label="test pipeline weekly advert contacts"),
            sync_advert_contacts = True,
        )
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="pipeline_upload_test"
        ),
        membership_group_configuration=MembershipGroupConfiguration(
            membership_group_csv_urls={ "listening_group": [
                "gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/test-pipeline-engagement-db-listening-group.csv"
            ]
            },
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["s01e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01e01_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("s01e01"),
                        analysis_dataset="s01e01"
                    )
                ],
            rapid_pro_non_relevant_field=ContactField(key="test_s01e01_non_relevant_contacts",
                                          label = "test s01e01 non relevant contacts"),
            ),
            OperatorDatasetConfiguration(
                raw_dataset="operator_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("operator"),
                        analysis_dataset="operator"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["gender"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="gender_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("gender"),
                        analysis_dataset="gender"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["location"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="location_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("kenya_county"),
                        analysis_dataset="kenya_county",
                        analysis_location=AnalysisLocations.KENYA_COUNTY
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("kenya_constituency"),
                        analysis_dataset="kenya_constituency",
                        analysis_location=AnalysisLocations.KENYA_CONSTITUENCY
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["age"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="age_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("age"),
                        analysis_dataset="age"
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("age_category"),
                        analysis_dataset="age_category",
                        age_category_config=AgeCategoryConfiguration(
                            age_analysis_dataset="age",
                            categories={
                                (10, 14): "10 to 14",
                                (15, 17): "15 to 17",
                                (18, 35): "18 to 35",
                                (36, 54): "36 to 54",
                                (55, 99): "55 to 99"
                            }
                        )
                    ),
                ],
            )
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
        cross_tabs=[
            ("age_category", "gender"),
        ],
        traffic_labels=[
            TrafficLabel(isoparse("2021-04-01T00:00+03:00"), isoparse("2021-05-01T00:00+03:00"), "April"),
            TrafficLabel(isoparse("2021-05-01T00:00+03:00"), isoparse("2021-06-01T00:00+03:00"), "May")
        ]
    ),
    archive_configuration = ArchiveConfiguration(
        archive_upload_bucket = "gs://pipeline-execution-backup-archive",
        bucket_dir_path =  "2021/TEST-PIPELINE_DB"
    )
)
