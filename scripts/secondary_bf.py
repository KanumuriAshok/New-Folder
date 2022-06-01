import sys

from qgis.core import *
from qgis.analysis import QgsNativeAlgorithms

QgsApplication.setPrefixPath('C:/Program Files/QGIS 3.20.0/apps/qgis', True)
qgs = QgsApplication([], False)
qgs.initQgis()

# Add the path to processing so we can import it next
# sys.path.append(r'C:/OSGeo4W64/apps/qgis/python/plugins')
sys.path.append('C:/Program Files/QGIS 3.20.0/apps/qgis/python/plugins')
# Imports usually should be at the top of a script but this unconventional
# order is necessary here because QGIS has to be initialized first

import processing
from processing.core.Processing import Processing
from processing.script import ScriptUtils
from config import *



class Secondary_bf(QgsProcessingAlgorithm):

    
    def processAlgorithm(self, parameters):
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        Processing.initialize()
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingFeedback()
        results = {}
        outputs = {}

        # brown_2605_1
        alg_params = {
            'demand': parameters['demand'],
            'duct': parameters['duct'],
            'googlepoles': parameters['googlepoles'],
            'landboundary': parameters['landboundary'],
            'piastruc': parameters['piastruc'],
            'streetlines': parameters['streetlines'],
            'native:convexhull_1:asnboundary': parameters['Asn_boundary'],
            'native:difference_2:lb_ug': parameters['Lb_ug'],
            'native:extractbyexpression_4:MDU_medium': parameters['Mdu_medium'],
            'native:extractbyexpression_5:Large_MDU': parameters['Largemdu'],
            'native:joinattributesbylocation_2:WITHLEAD': parameters['Withleading'],
            'native:joinattributesbylocation_4:aireal_drop': parameters['Aireal_drop'],
            'script:clustergrpouing_u_1:estimated_sn': parameters['E_nodes'],
            'script:clustergrpouing_u_1:ug_cluster': parameters['Ug_cluster']
        }
        outputs['Brown_2605_1'] = processing.run('model:brown_2605_1', alg_params)
        results['Aireal_drop'] = outputs['Brown_2605_1']['native:joinattributesbylocation_4:aireal_drop']
        results['Asn_boundary'] = outputs['Brown_2605_1']['native:convexhull_1:asnboundary']
        results['E_nodes'] = outputs['Brown_2605_1']['script:clustergrpouing_u_1:estimated_sn']
        results['Largemdu'] = outputs['Brown_2605_1']['native:extractbyexpression_5:Large_MDU']
        results['Lb_ug'] = outputs['Brown_2605_1']['native:difference_2:lb_ug']
        results['Mdu_medium'] = outputs['Brown_2605_1']['native:extractbyexpression_4:MDU_medium']
        results['Ug_cluster'] = outputs['Brown_2605_1']['script:clustergrpouing_u_1:ug_cluster']
        results['Withleading'] = outputs['Brown_2605_1']['native:joinattributesbylocation_2:WITHLEAD']
        return results

obj = Secondary_bf()
res = obj.processAlgorithm({
    'Asn_boundary':fr"{HOME_DIR}/{sys.argv[1]}/data output/asn_boundary.shp",\
    'Aireal_drop':fr"{HOME_DIR}/{sys.argv[1]}/data output/aerial_drop.shp",\
    'Lb_ug':fr"{HOME_DIR}/{sys.argv[1]}/data output/ug_landboundary.shp",\
    'E_nodes':fr"{HOME_DIR}/{sys.argv[1]}/data output/estimated_nodes.shp",\
    'Largemdu':fr"{HOME_DIR}/{sys.argv[1]}/data output/large_mdu.shp",\
    'Ug_cluster':fr"{HOME_DIR}/{sys.argv[1]}/data output/cluster_output.shp",\
    'Withleading':fr"{HOME_DIR}/{sys.argv[1]}/data output/withleading.shp",\
    'Mdu_medium':fr"{HOME_DIR}/{sys.argv[1]}/data output/wall_mount_mdu.shp",\
    'demand':fr"{HOME_DIR}/{sys.argv[1]}/data_input/demandpoints.shp",\
    'duct':fr"{HOME_DIR}/{sys.argv[1]}/data_input/duct.shp",\
    'landboundary':fr"{HOME_DIR}/{sys.argv[1]}/data_input/landoundary.shp",\
    'googlepoles':fr"{HOME_DIR}/{sys.argv[1]}/data_input/polesfromgoogle.shp",\
    'piastruc':fr"{HOME_DIR}/{sys.argv[1]}/data_input/piastruc.shp",\
    'streetlines':fr"{HOME_DIR}/{sys.argv[1]}/data_input/streetlines.shp",
})
print(res)