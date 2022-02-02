# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 16:50:13 2022

@author: yalha
"""

import os
import json
if not os.path.exists("../.config/"):
    os.makedirs("../.config/")
    
if not os.path.exists("../.cache/"):
    os.makedirs("../.cache/")

with open("../.config/keys", "a+") as fout:
    pass

with open("../.config/terms", "a+") as fout:
    pass
    
with open("../.config/lang_detect", "w") as fout:
    fout.write("{}\n".format(json.dumps({"file": "PATH"}, ensure_ascii=False)))
    


