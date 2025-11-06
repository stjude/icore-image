.PHONY: all signed clean deps deps-python deps-deid deps-electron test dev
.PHONY: external-deps jre8 dcmtk build-binaries build-icorecli build-django-app
.PHONY: prepare-assets build-dmg build-dmg-signed

.DEFAULT_GOAL := all

test:
	@docker info > /dev/null 2>&1 || (echo "Error: Docker is not running. Please start Docker and try again." && exit 1)
	pytest -v
	cd electron && npm test -- --verbose

dev: external-deps
	@echo "Starting iCore in development mode..."
	@if [ "$$(uname -s)" = "Linux" ]; then \
		export JAVA_HOME=$$(pwd)/jre8; \
	else \
		export JAVA_HOME=$$(pwd)/jre8/Contents/Home; \
	fi && \
	export DCMTK_HOME=$$(pwd)/dcmtk && \
	cd electron && npm start

deps: deps-python deps-deid deps-electron

deps-python:
	pip install -r requirements.txt
	python -m spacy download en_core_web_sm

deps-deid:
	cd deid && npm install

deps-electron:
	cd electron && npm install

jre8:
	@if [ ! -d "jre8" ]; then \
		echo "Downloading JRE8..."; \
		if [ "$$(uname -s)" = "Linux" ]; then \
			curl -s "https://api.adoptium.net/v3/assets/feature_releases/8/ga?os=linux&architecture=x64&image_type=jre&jvm_impl=hotspot" \
			| jq -r '.[] | .binaries[] | select(.image_type=="jre") | .package.link' \
			| head -n1 \
			| xargs curl -L \
			| tar -xz; \
			mv jdk8*-jre jre8; \
		else \
			curl -s "https://api.adoptium.net/v3/assets/feature_releases/8/ga?os=mac&architecture=x64&image_type=jre&jvm_impl=hotspot" \
			| jq -r '.[] | .binaries[] | select(.image_type=="jre") | .package.link' \
			| head -n1 \
			| xargs curl -L \
			| tar -xj; \
			mv jdk8*-jre jre8; \
		fi; \
	else \
		echo "JRE8 already exists"; \
	fi

dcmtk:
	@if [ ! -d "dcmtk" ]; then \
		echo "Downloading DCMTK..."; \
		if [ "$$(uname -s)" = "Linux" ]; then \
			curl -L https://dicom.offis.de/download/dcmtk/release/bin/dcmtk-3.6.9-linux-x86_64.tar.bz2 | tar -xj; \
			mv dcmtk-3.6.9-linux-x86_64 dcmtk; \
			cd dcmtk/bin && find . -type f ! -name 'findscu' ! -name 'movescu' -delete; \
		else \
			curl -L https://dicom.offis.de/download/dcmtk/dcmtk369/bin/dcmtk-3.6.9-macosx-x86_64.tar.bz2 | tar -xj; \
			mv dcmtk-3.6.9-macosx-x86_64 dcmtk; \
			cd dcmtk/bin && find . -type f ! -name 'findscu' ! -name 'movescu' -delete; \
		fi; \
	else \
		echo "DCMTK already exists"; \
	fi

external-deps: jre8 dcmtk

build-icorecli:
	rm -rf dist
	pyinstaller --clean -y icorecli.spec

build-django-app:
	cd deid && \
		pyinstaller --clean -y manage.spec && \
		pyinstaller --clean -y initialize_admin_password.spec

build-binaries: build-icorecli build-django-app

prepare-assets:
	rm -rf electron/assets/dist
	cp deid/home/settings.json electron/assets
	ditto deid/dist electron/assets/dist
	ditto dist/icorecli electron/assets/dist/icorecli

build-dmg:
	cd electron && CSC_IDENTITY_AUTO_DISCOVERY=false npm run build
	@VERSION=$$(node -p "require('./electron/package.json').version"); \
	DMG_FILE=$$(ls -t electron/dist/iCore-*.dmg 2>/dev/null | head -1); \
	cp "$$DMG_FILE" icore-x64-$$VERSION.dmg; \
	echo "DMG copied to icore-x64-$$VERSION.dmg"

build-dmg-signed:
	@if [ -z "$$APPLE_ID" ] || [ -z "$$APPLE_APP_SPECIFIC_PASSWORD" ] || [ -z "$$APPLE_TEAM_ID" ]; then \
		echo "Error: Missing required environment variables for notarization:"; \
		[ -z "$$APPLE_ID" ] && echo "  - APPLE_ID"; \
		[ -z "$$APPLE_APP_SPECIFIC_PASSWORD" ] && echo "  - APPLE_APP_SPECIFIC_PASSWORD"; \
		[ -z "$$APPLE_TEAM_ID" ] && echo "  - APPLE_TEAM_ID"; \
		echo ""; \
		echo "Please set these variables before running signed builds:"; \
		echo "  export APPLE_ID=\"your-apple-id@example.com\""; \
		echo "  export APPLE_APP_SPECIFIC_PASSWORD=\"your-app-specific-password\""; \
		echo "  export APPLE_TEAM_ID=\"your-team-id\""; \
		exit 1; \
	fi
	cd electron && npm run build_signed
	@VERSION=$$(node -p "require('./electron/package.json').version"); \
	DMG_FILE=$$(ls -t electron/dist/iCore-*.dmg 2>/dev/null | head -1); \
	cp "$$DMG_FILE" icore-x64-$$VERSION.dmg; \
	echo "DMG copied to icore-x64-$$VERSION.dmg"

clean:
	rm -rf dist deid/dist electron/assets/dist build deid/build
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

all: deps external-deps build-binaries prepare-assets build-dmg

signed: deps external-deps build-binaries prepare-assets build-dmg-signed

