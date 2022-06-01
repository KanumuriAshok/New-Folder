import sys

from qgis.core import *
from qgis.analysis import QgsNativeAlgorithms

QgsApplication.setPrefixPath('C:/Program Files/QGIS 3.20.0/apps/qgis', True)
qgs = QgsApplication([], False)
qgs.initQgis()

# Add the path to processing so we can import it next
# sys.path.append(r'C:\OSGeo4W64\apps\qgis\python\plugins')
sys.path.append('C:/Program Files/QGIS 3.20.0/apps/qgis/python/plugins')
# Imports usually should be at the top of a script but this unconventional
# order is necessary here because QGIS has to be initialized first

import processing
from processing.core.Processing import Processing
from config import *
from processing.script import ScriptUtils

# Processing path for standalone - C:\Users\jyothy\AppData\Roaming\python\profiles\default\processing
# Processing path for desktop app - C:\Users\jyothy\AppData\Roaming\QGIS\QGIS3\profiles\default\processing

# Processing.initialize()
# print("[INFO] Folder for script algorithms:", ScriptUtils.scriptsFolders())
# print("[INFO] Script algorithms available:",
#     [s.displayName() for s in QgsApplication.processingRegistry().providerById("script").algorithms()])



class Secondary_drop_ug(QgsProcessingAlgorithm):

    
    def processAlgorithm(self, parameters):
        Processing.initialize()
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingFeedback()
        results = {}
        outputs = {}

        # dropntwrk_ug
        alg_params = {
            'demand': parameters['demand'],
            'gaist': parameters['gaist'],
            'qgis:distancetonearesthublinetohub_1:leadin': parameters['Lead_in']
        }
        outputs['Dropntwrk_ug'] = processing.run('model:dropntwrk_ug', alg_params)
        results['Lead_in'] = outputs['Dropntwrk_ug']['qgis:distancetonearesthublinetohub_1:leadin']
        return results
obj = Secondary_drop_ug()
res = obj.processAlgorithm({
    'demand':fr"{HOME_DIR}/{sys.argv[1]}/data_input/demand.shp",
    'gaist':fr"{HOME_DIR}/{sys.argv[1]}/data_input/gaist.shp",
    'Lead_in':fr"{HOME_DIR}/{sys.argv[1]}/data output/lead_in.shp",
   
    
})
print(res)
