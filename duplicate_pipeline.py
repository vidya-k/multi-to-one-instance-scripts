def get_PipeLines(collectionName):
        if collectionName=='pms-role':
                    return [
                         {
                        '$group': {
                         '_id': {'roleName': '$roleName'},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_designation':
                    return [
                         {
                        '$group': {
                         '_id': {'designationId': '$designationId'},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_modeofentry':
                    return [
                         {
                        '$group': {
                         '_id': {'modeOfEntry': '$modeOfEntry'},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_usertype':
                    return [
                         {
                        '$group': {
                         '_id': {'userType': '$userType'},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ] 
        if collectionName=='year-of-passing':
                    return [
                         {
                        '$group': {
                         '_id': {'year': '$year'},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]             
        if collectionName=='dhi_component_wise_config':
                    return [
                         {
                        '$group': {
                         '_id': {'componentType': '$componentType',"componentName":"$componentName"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_salary_template':
                    return [
                         {
                        '$group': {
                         '_id': {'userType': '$userType',"employmentType":"$employmentType"
                                 ,"year":"$year","financialYear":"$financialYear"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_insurance_configuration':
                    return [
                         {
                        '$group': {
                         '_id': {'installmentCount': '$installmentCount',"insuranceTerm":"$insuranceTerm"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_financial_year_configuration':
                    return [
                         {
                        '$group': {
                         '_id': {'financialYear': '$financialYear',"startMonth":"$startMonth","endMonth":"$endMonth"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_tax_configuration':
                    return [
                         {
                        '$group': {
                         '_id': {'taxType': '$taxType',"itSchemeIdentifier":"$itSchemeIdentifier","applicableTo":"$applicableTo"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_deduction_schemes':
                    return [
                         {
                        '$group': {
                         '_id': {'deductionType': '$deductionType',"deductionPlan":"$deductionPlan"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_payroll_leave_config':
                    return [
                         {
                        '$group': {
                         '_id': {'lopCateogryName': '$lopCateogryName'},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_salary_structure_configuration':
                    return [
                         {
                        '$group': {
                         '_id': {'financialYear': '$financialYear'},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_nomenclature_configuration':
                    return [
                         {
                        '$group': {
                         '_id': {'roleName': '$roleName',"userType":"$userType","microFrontend":"$microFrontend","microFrontEndType" :"$microFrontEndType"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]

        if collectionName=='dhi_feature':
                    return [
                         {
                        '$group': {
                         '_id': {"referenceKey" : '$referenceKey'},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_labia_parameter':
                    return [
                         {
                        '$group': {
                         '_id': {"name" : '$name'},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]       
        if collectionName=='dhi_lab_marks_parameters':
                    return [
                         {
                        '$group': {
                         '_id':  '$name',
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]       
        if collectionName=='dhi_routing_config':
                    return [
                         {
                        '$group': {
                         '_id':  '$routingUrl',
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_term_detail' or collectionName=='dhi_ia_configuration' or collectionName=='dhi_student_attendance_configuration_refactored' or collectionName=='dhi_master_feedback_config':
                    return [
                         {
                        '$group': {
                         '_id': {'academicYear': '$academicYear',"degreeId":"$degreeId","degreeBatch":"$degreeBatch"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_class_transfer_configuration' or collectionName=='dhi_lesson_plan_config' or collectionName=='dhi_master_feedback':
                    return [
                         {
                        '$group': {
                         '_id': {"degreeId":"$degreeId","degreeBatch":"$degreeBatch"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_holiday_detail':
                    return [
                         {
                        '$group': {
                         '_id': {'calendarYear': '$calendarYear',"degreeId":"$degreeId","reasonForHoliday":"$reasonForHoliday"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]               
        if collectionName=='dhi_scheme':
                    return [
                         {
                        '$group': {
                         '_id': {"degreeId":"$degreeId","scheme":"$scheme"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_form_configuration':
                    return [
                      {
                        '$unwind': "$degreeIds"
                        },
                         {
                        '$group': {
                         '_id': {"degreeIds":"$degreeIds","formName":"$formName"},
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]                                
        if collectionName=='dhi_student_data_configuration':
                    return [
                         {
                        '$group': {
                         '_id': "$degreeId",
                        'duplicates': {'$addToSet': '$_id'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]
        if collectionName=='dhi_coursedata':
                    return [
                     {
                        '$unwind': "$departments"
                        },
                         {
                        '$group': {
                         '_id': {"degreeId":"$degreeId","scheme":"$scheme"},
                        'duplicates': {'$addToSet': '$_id'},
                        'departments':{'$addToSet': '$departments'},
                         'count': {'$sum': 1}
                            }
                        },
                        {
                         '$match': {
                          'count': {'$gt': 1}
                             }
                        }
                        ]                                                                                                  
