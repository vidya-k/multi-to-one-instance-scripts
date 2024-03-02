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
        
