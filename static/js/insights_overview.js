document.addEventListener('DOMContentLoaded', function() {
    const proceedButton = document.getElementById('proceedButton');
    const stateCode = proceedButton.dataset.state;
    const stateName = proceedButton.dataset.stateName;
    const npiType = proceedButton.dataset.npiType;
    
    // Background preloading removed to prevent issues
    
    // Handle proceed button click
    proceedButton.addEventListener('click', function() {
        if (this.classList.contains('loading')) return;
        
        // Show loading state
        this.classList.add('loading');
        this.innerHTML = '<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>Loading...';
        
        // Show skeleton screen immediately for instant feedback
        showSkeletonScreen();
        
        // Navigate to insights page after a short delay
        setTimeout(() => {
            const npiParam = npiType ? `?npi_type=${npiType}` : '';
            window.location.href = `/commercial/insights/${stateCode}/${npiParam}`;
        }, 500); // Slightly longer delay to show skeleton
    });
    
    function showSkeletonScreen() {
        // Create and show enhanced loading screen with progress indicators
        const skeleton = document.createElement('div');
        skeleton.id = 'skeletonOverlay';
        skeleton.style.cssText = `
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            z-index: 9999;
            overflow-y: auto;
        `;
        
        // Add enhanced loading content with progress indicators and tips
        skeleton.innerHTML = `
            <div class="skeleton-container">
                <!-- Progress Bar -->
                <div class="progress-bar">
                    <div class="progress-fill"></div>
                </div>
                
                <!-- Loading Header -->
                <div class="skeleton-header">
                    <div class="text-center mb-4">
                        <div class="loading-logo mb-3">
                            <i class="fas fa-chart-line fa-3x text-primary"></i>
                        </div>
                        <h2 class="text-primary mb-2">Loading ${stateName} Insights</h2>
                        <p class="text-muted">Preparing interactive visualizations and analytics...</p>
                    </div>
                </div>
                
                <!-- Progress Steps -->
                <div class="progress-steps mb-4">
                    <div class="step active" id="step1">
                        <div class="step-icon">
                            <i class="fas fa-database"></i>
                        </div>
                        <div class="step-content">
                            <h6>Loading Data</h6>
                            <p>Accessing ${stateName} commercial rates database</p>
                        </div>
                    </div>
                    <div class="step" id="step2">
                        <div class="step-icon">
                            <i class="fas fa-filter"></i>
                        </div>
                        <div class="step-content">
                            <h6>Preparing Filters</h6>
                            <p>Setting up procedure classes, groups, and billing codes</p>
                        </div>
                    </div>
                    <div class="step" id="step3">
                        <div class="step-icon">
                            <i class="fas fa-chart-bar"></i>
                        </div>
                        <div class="step-content">
                            <h6>Building Charts</h6>
                            <p>Creating interactive visualizations and analytics</p>
                        </div>
                    </div>
                    <div class="step" id="step4">
                        <div class="step-icon">
                            <i class="fas fa-rocket"></i>
                        </div>
                        <div class="step-content">
                            <h6>Ready!</h6>
                            <p>Your insights dashboard is ready to explore</p>
                        </div>
                    </div>
                </div>
                
                <!-- Loading Tips -->
                <div class="loading-tips-container mb-4">
                    <h5 class="text-center mb-3">
                        <i class="fas fa-lightbulb text-warning"></i> 
                        While You Wait - Pro Tips
                    </h5>
                    <div class="tips-carousel">
                        <div class="tip active" id="tip1">
                            <div class="tip-content">
                                <h6><i class="fas fa-filter text-primary me-2"></i>Start with Procedure Class</h6>
                                <p>Begin filtering with broad categories like 'Surgery' or 'Medicine' to get focused results</p>
                            </div>
                        </div>
                        <div class="tip" id="tip2">
                            <div class="tip-content">
                                <h6><i class="fas fa-layer-group text-success me-2"></i>Drill Down with Groups</h6>
                                <p>Use procedure groups for more targeted analysis after selecting a class</p>
                            </div>
                        </div>
                        <div class="tip" id="tip3">
                            <div class="tip-content">
                                <h6><i class="fas fa-dollar-sign text-warning me-2"></i>Set Rate Thresholds</h6>
                                <p>Filter by rates over $100+ to focus on significant procedures</p>
                            </div>
                        </div>
                        <div class="tip" id="tip4">
                            <div class="tip-content">
                                <h6><i class="fas fa-hospital text-info me-2"></i>Compare Billing Classes</h6>
                                <p>Professional rates are typically lower than facility rates. ASC rates often provide cost-effective alternatives.</p>
                            </div>
                        </div>
                        <div class="tip" id="tip5">
                            <div class="tip-content">
                                <h6><i class="fas fa-map-marker-alt text-success me-2"></i>Geographic Insights</h6>
                                <p>Use CBSA regions to compare metropolitan vs. rural areas and identify regional variations</p>
                            </div>
                        </div>
                    </div>
                    <div class="tips-nav text-center mt-3">
                        <button class="btn btn-sm btn-outline-primary" onclick="showTip(1)">1</button>
                        <button class="btn btn-sm btn-outline-primary" onclick="showTip(2)">2</button>
                        <button class="btn btn-sm btn-outline-primary" onclick="showTip(3)">3</button>
                        <button class="btn btn-sm btn-outline-primary" onclick="showTip(4)">4</button>
                        <button class="btn btn-sm btn-outline-primary" onclick="showTip(5)">5</button>
                    </div>
                </div>
                
                <!-- Loading Spinner -->
                <div class="loading-spinner text-center">
                    <div class="spinner mb-3"></div>
                    <p class="text-muted">Preparing your insights experience...</p>
                </div>
            </div>
        `;
        
        // Add enhanced skeleton styles
        const skeletonStyles = document.createElement('style');
        skeletonStyles.textContent = `
            .skeleton-container {
                max-width: 1000px;
                margin: 0 auto;
                padding: 20px;
            }
            
            .skeleton-header {
                background: white;
                border-radius: 12px;
                padding: 30px;
                margin-bottom: 20px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                text-align: center;
            }
            
            .loading-logo {
                animation: pulse 2s infinite;
            }
            
            @keyframes pulse {
                0% { transform: scale(1); }
                50% { transform: scale(1.1); }
                100% { transform: scale(1); }
            }
            
            .progress-steps {
                background: white;
                border-radius: 12px;
                padding: 25px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            }
            
            .step {
                display: flex;
                align-items: center;
                margin-bottom: 20px;
                padding: 15px;
                border-radius: 8px;
                transition: all 0.3s ease;
                opacity: 0.5;
            }
            
            .step.active {
                opacity: 1;
                background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
                border-left: 4px solid #2196f3;
            }
            
            .step.completed {
                opacity: 0.8;
                background: linear-gradient(135deg, #e8f5e8 0%, #c8e6c9 100%);
                border-left: 4px solid #4caf50;
            }
            
            .step-icon {
                width: 50px;
                height: 50px;
                background: linear-gradient(135deg, #2196f3, #1976d2);
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                color: white;
                margin-right: 20px;
                flex-shrink: 0;
            }
            
            .step.completed .step-icon {
                background: linear-gradient(135deg, #4caf50, #388e3c);
            }
            
            .step-content h6 {
                margin: 0 0 5px 0;
                color: #1976d2;
                font-weight: 600;
            }
            
            .step-content p {
                margin: 0;
                color: #666;
                font-size: 0.9rem;
            }
            
            .loading-tips-container {
                background: white;
                border-radius: 12px;
                padding: 25px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            }
            
            .tips-carousel {
                position: relative;
                min-height: 120px;
            }
            
            .tip {
                display: none;
                padding: 20px;
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
                border-radius: 8px;
                border-left: 4px solid #007bff;
            }
            
            .tip.active {
                display: block;
                animation: fadeIn 0.5s ease-in;
            }
            
            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(10px); }
                to { opacity: 1; transform: translateY(0); }
            }
            
            .tip-content h6 {
                margin: 0 0 10px 0;
                color: #495057;
                font-weight: 600;
            }
            
            .tip-content p {
                margin: 0;
                color: #6c757d;
                font-size: 0.9rem;
                line-height: 1.5;
            }
            
            .tips-nav button {
                margin: 0 5px;
                width: 35px;
                height: 35px;
                border-radius: 50%;
                padding: 0;
            }
            
            .tips-nav button.active {
                background: #007bff;
                color: white;
                border-color: #007bff;
            }
            
            .loading-spinner {
                background: white;
                border-radius: 12px;
                padding: 30px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            }
            
            .spinner {
                width: 50px;
                height: 50px;
                border: 5px solid #f3f3f3;
                border-top: 5px solid #007bff;
                border-radius: 50%;
                animation: spin 1s linear infinite;
                margin: 0 auto;
            }
            
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
            
            .progress-bar {
                width: 100%;
                height: 6px;
                background: #e9ecef;
                border-radius: 3px;
                overflow: hidden;
                margin-bottom: 20px;
                position: sticky;
                top: 0;
                z-index: 1000;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            
            .progress-fill {
                height: 100%;
                background: linear-gradient(90deg, #007bff, #0056b3, #28a745);
                border-radius: 3px;
                transition: width 0.5s ease;
                animation: progress 3s ease-in-out infinite;
            }
            
            @keyframes progress {
                0% { width: 0%; }
                25% { width: 30%; }
                50% { width: 60%; }
                75% { width: 85%; }
                100% { width: 100%; }
            }
        `;
        
        document.head.appendChild(skeletonStyles);
        document.body.appendChild(skeleton);
        
        // Start progress animation
        startProgressAnimation();
        
        // Start step progression
        startStepProgression();
        
        // Start tips rotation
        startTipsRotation();
    }
    
    function startProgressAnimation() {
        const progressFill = document.querySelector('.progress-fill');
        let progress = 0;
        const progressInterval = setInterval(() => {
            progress += Math.random() * 8 + 2;
            if (progress >= 100) {
                progress = 100;
                clearInterval(progressInterval);
            }
            progressFill.style.width = progress + '%';
        }, 200);
    }
    
    function startStepProgression() {
        const steps = ['step1', 'step2', 'step3', 'step4'];
        let currentStep = 0;
        
        const stepInterval = setInterval(() => {
            if (currentStep > 0) {
                document.getElementById(steps[currentStep - 1]).classList.add('completed');
                document.getElementById(steps[currentStep - 1]).classList.remove('active');
            }
            
            if (currentStep < steps.length) {
                document.getElementById(steps[currentStep]).classList.add('active');
            }
            
            currentStep++;
            
            if (currentStep >= steps.length) {
                clearInterval(stepInterval);
            }
        }, 1500);
    }
    
    function startTipsRotation() {
        const tips = ['tip1', 'tip2', 'tip3', 'tip4', 'tip5'];
        let currentTip = 0;
        
        const tipInterval = setInterval(() => {
            tips.forEach(tip => {
                document.getElementById(tip).classList.remove('active');
            });
            
            document.getElementById(tips[currentTip]).classList.add('active');
            
            // Update nav buttons
            document.querySelectorAll('.tips-nav button').forEach((btn, index) => {
                btn.classList.toggle('active', index === currentTip);
            });
            
            currentTip = (currentTip + 1) % tips.length;
        }, 4000);
    }
    
    window.showTip = function(tipNumber) {
        const tips = ['tip1', 'tip2', 'tip3', 'tip4', 'tip5'];
        tips.forEach(tip => {
            document.getElementById(tip).classList.remove('active');
        });
        
        document.getElementById(tips[tipNumber - 1]).classList.add('active');
        
        // Update nav buttons
        document.querySelectorAll('.tips-nav button').forEach((btn, index) => {
            btn.classList.toggle('active', index === tipNumber - 1);
        });
    };
    
    // All preloading functions removed to prevent issues
});

