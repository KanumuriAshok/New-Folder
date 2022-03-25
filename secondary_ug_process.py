import sys

from qgis.core import *
from qgis.analysis import QgsNativeAlgorithms

QgsApplication.setPrefixPath('C:/Program Files/QGIS 3.20.3/apps/qgis', True)
qgs = QgsApplication([], False)
qgs.initQgis()

# Add the path to processing so we can import it next
# sys.path.append(r'C:\OSGeo4W64\apps\qgis\python\plugins')
sys.path.append('C:/Program Files/QGIS 3.20.3/apps/qgis/python/plugins')
# Imports usually should be at the top of a script but this unconventional
# order is necessary here because QGIS has to be initialized first

import processing
from processing.core.Processing import Processing
from processing.script import ScriptUtils
class Secondary_ug(QgsProcessingAlgorithm):

    def processAlgorithm(self, parameters):
        Processing.initialize()
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingFeedback()
        results = {}
        outputs = {}

        # clustergrpouing_u
        alg_params = {
            'VERBOSE_LOG': False,
            'demandpoints': parameters['demand'],
            'streetlines': parameters['street'],
            'Ug_cluster': parameters['Ug_cluster'],
            'Ug_est_nodes': parameters['Est_nodes'],
            'Ug_outl': parameters['Ug_outliers']
        }
        outputs['Clustergrpouing_u'] = processing.run('script:clustergrpouing_u', alg_params)
        results['Est_nodes'] = outputs['Clustergrpouing_u']['Ug_est_nodes']
        results['Ug_cluster'] = outputs['Clustergrpouing_u']['Ug_cluster']
        results['Ug_outliers'] = outputs['Clustergrpouing_u']['Ug_outl']
        return results
        
obj = Secondary_ug()
res = obj.processAlgorithm({
    'demand':r"C:\Users\jyothy\Desktop\New folder\geoflask\data_input\demandpoints.shp",
    'street':r"C:\Users\jyothy\Desktop\New folder\geoflask\data_input\streetlines.shp",
    'Ug_cluster':r"C:\Users\jyothy\Desktop\New folder\geoflask\data output\cluster_output.shp",
    'Est_nodes':r"C:\Users\jyothy\Desktop\New folder\geoflask\data output\nodes_output.shp",
    'Ug_outliers':r"C:\Users\jyothy\Desktop\New folder\geoflask\data output\outlier_output.shp"
})
print(res)