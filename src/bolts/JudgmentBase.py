import logging

logger = logging.getLogger(__file__)


class JudgmentBusiness(object):

    LEVEL1_CLASS = {
                        '1': 'PCSearch',
                        '2': 'PCUnion',
                        '3': 'WSSearch',
                        '5': 'WSUnion',
                        '6': 'PCGalaxy',
                        '7': 'WSGalaxy',
                        '8': 'INMethod'
                   }

    LEVEL2_CLASS = {
                        '101': ['1', 'SogouSearch'],
                        '102': ['1', 'SohuSearch'],
                        '103': ['1', 'BDSearch'],
                        '104': ['1', 'SogouExploer'],
                        '105': ['1', 'SogouInput'],
                        '106': ['1', 'SogouApi'],
                        '107': ['1', 'VerticalSearch'],
                        '201': ['2', 'THEME'],
                        '208': ['2', 'SearchKey'],
                        '211': ['2', 'PicSearch'],
                        '302': ['3', 'QQ'],
                        '303': ['3', 'ZiYou'],
                        '304': ['3', 'WaiGou'],
                        '501': ['5', 'ImageText'],
                        '503': ['5', 'SearchKey'],
                        '510': ['5', 'APPImageText'],
                        '511': ['5', 'APPSearchKey'],
                        '520': ['5', 'SogouImage'],
                        '530': ['5', 'DiscountCoupon'],
                        '601': ['6', 'ResultsPage'],
                        '602': ['6', 'MiddlePage'],
                        '205': ['6', 'MicroPortal'],
                        '207': ['6', 'PCShoppingSearch'],
                        '701': ['7', 'ResultsPage'],
                        '702': ['7', 'MiddlePage'],
                        '507': ['7', 'WSShoppingSearch'],
                        '801': ['8', 'InputStream'],
                        '802': ['8', 'DIRECT'],
                        '803': ['8', 'PreSearchRecomm']
                   }
    LEVEL3_CLASS = {
                        '10100': ['101', 'SOGOU'],
                        '10101': ['101', 'SogouFree'],
                        '10103': ['101', 'SosoZiyou'],
                        '10200': ['102', 'SOHU'],
                        '10201': ['102', 'SohuFree'],
                        '10300': ['103', 'BDExtend'],
                        '10301': ['103', 'IndexRecomm'],
                        '10302': ['103', 'HotWord'],
                        '10303': ['103', '123Buy'],
                        '10304': ['103', 'InputLingxi'],
                        '10400': ['104', 'ExploerDirectSearch'],
                        '10401': ['104', 'ExploerInitiativeSearch'],
                        '10500': ['105', 'INPUT'],
                        '10600': ['106', 'API'],
                        '10601': ['106', 'BING'],
                        '10701': ['107', 'SogouKnowledge'],
                        '10702': ['107', 'SosoWenwen'],
                        '10703': ['107', 'PicSearch'],
                        '10704': ['107', 'WenWen'],
                        '20100': ['201', 'THEME'],
                        '20800': ['208', 'SearchKey'],
                        '21100': ['211', 'ImageSearch'],
                        '30200': ['302', 'QQExploer'],
                        '30201': ['302', 'PhoneTecent'],
                        '30202': ['302', 'PhoneWenwen'],
                        '30203': ['302', 'SosoWap'],
                        '30204': ['302', 'QQExploerLow'],
                        '30209': ['302', 'TecentOther'],
                        '30301': ['303', 'SogouWap'],
                        '30302': ['303', 'SogouKnowledge'],
                        '30303': ['303', 'SogouTransCode'],
                        '30304': ['303', 'SogouOther'],
                        '30305': ['303', 'SogouAndVertical'],
                        '30306': ['303', 'PhoneSohu'],
                        '30307': ['303', 'PhoneLingxi'],
                        '30400': ['304', 'WaigouApi'],
                        '30401': ['304', 'PhoneMakers'],
                        '30402': ['304', 'ExternalChange'],
                        '30403': ['304', 'APPEmbedded'],
                        '30404': ['304', 'WSExploer'],
                        '30405': ['304', 'OfficialOperator'],
                        '30406': ['304', 'WirelessOperator'],
                        '30407': ['304', 'WAPWebSite'],
                        '30408': ['304', 'BDS'],
                        '30409': ['304', 'OTHER'],
                        '50101': ['501', 'ImageText'],
                        '50170': ['501', 'DirectInvestImageText'],
                        '50300': ['503', 'SearchKey'],
                        '50760': ['507', 'MoonShoppingSearch'],
                        '51000': ['510', 'APPImageText'],
                        '51070': ['510', 'DirectInvestAPPImageText'],
                        '51100': ['511', 'APPSearchKey'],
                        '52000': ['520', 'SogouImage'],
                        '53071': ['530', 'DirectInvestWAPDiscountCoupon'],
                        '53072': ['530', 'DirectInvestAPPDiscountCoupon'],
                        '60180': ['601', 'ResultsPage'],
                        '60280': ['602', 'MiddlePage'],
                        '20560': ['205', 'MoonMicroPortal'],
                        '20760': ['207', 'MoonPCShoppingSearch'],
                        '70100': ['701', 'ResultsPage'],
                        '70180': ['701', 'GalaxyResultsPage'],
                        '70200': ['702', 'MiddlePage'],
                        '70280': ['702', 'GalaxyMiddlePage'],
                        '80100': ['801', 'InputStream'],
                        '80200': ['802', 'DIRECT'],
                        '80300': ['803', 'PreSearchRecomm']
                   }


    @classmethod
    def judgment_business(cls, business_code):

        try:
            level3_class_info = cls.LEVEL3_CLASS[business_code]
        except KeyError:
            return ['UNKNOWN', 'UNKNOWN', 'UNKNOWN']
            
        level2_class_info = cls.LEVEL2_CLASS[level3_class_info[0]]
        level1_class_info = cls.LEVEL1_CLASS[level2_class_info[0]]


        logger.info("class 3 : [%s,%s]"%(type(level3_class_info),level3_class_info))
        logger.info("class 2 : [%s,%s]"%(type(level2_class_info),level2_class_info))
        logger.info("class 1 : [%s,%s]"%(type(level1_class_info),level1_class_info))
        level1_class_name = level1_class_info if level1_class_info else None
        level2_class_name = level2_class_info[1] if level2_class_info else None
        level3_class_name = level3_class_info[1] if level3_class_info else None

        logger.info("class 3 : [%s]"%(type(level3_class_name),))
        logger.info("class 2 : [%s]"%(type(level2_class_name),))
        logger.info("class 1 : [%s]"%(type(level1_class_name),))

        return [level1_class_name, level2_class_name, level3_class_name]

                

class JudgmentFilterCode(object):

    MAP_FILTER_CODE = {
                    '0':'PASS',
                    '1':'BadUrl',
                    '2':'TimeOut',
                    '3':'BlackRequest',
                    '4':'DiffRegion',
                    '5':'COOKIE',
                    '6':'COOKIE',
                    '7':'COOKIE',
                    '8':'ProxyUserAgentError',
                    '9':'ProxyUserAgentError',
                    '10':'RefererNull',
                    '11':'FreeUrl',
                    '12':'TextUnionParamPid',
                    '13':'TextUnionParamPid',
                    '18':'OTHER',
                    '22':'OTHER',
                    '26':'OTHER',
                    '123':'NotCpcAd'
                }

    @classmethod
    def judgment_filter_code(cls, filter_code):

        try:
            query_result = cls.MAP_FILTER_CODE.get(filter_code, None)
            filter_name = 'OTHER' if query_result is None else query_result
            return filter_name
        except IndexError, KeyError:
            logger.error("Filter types do not exist[%s]"%(filter_code,))
            return None


class JudgmentPidLevel(object):

    @staticmethod
    def judgment_pid_level(pid, flag, reserved):
        if not (flag%2 and flag < 16):
            if flag%2 == 1:
                if pid in ['sogou', 'sohu'] or pid.startswith('sogou-'):
                    pidlevel = 'level_1'
                    levelgroup = 'total_level_1'
                else:
                    pidlevel = 'level_xml'
                    levelgroup = 'total_xml'
            else:
                pidlevel = 'level_{}'.format(str((reserved & 0x78000)/0x8000))
                levelgroup = 'total_{}'.format(pidlevel)

            return [pidlevel, levelgroup]
        else:
            logger.info("############# %s %s %s ##########"%(pid, flag, reserved))
            return None
