# SAM invokes this target because the resource logical id is DataProcessingLayer.
# Dependencies must be installed below python/ for Lambda Layers to import them.
build-DataProcessingLayer:
	mkdir -p "$(ARTIFACTS_DIR)/python"
	cp -R python/data_processing_framework "$(ARTIFACTS_DIR)/python/"
	python3 -m pip install --requirement requirements.txt --target "$(ARTIFACTS_DIR)/python" --upgrade
