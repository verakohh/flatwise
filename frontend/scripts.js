// --- Global Data and State ---
let currentRecommendations = [];

const REGION_LOOKUP = {
    "North": ["Woodlands", "Yishun", "Sembawang"],
    "North-East": ["Sengkang", "Punggol", "Hougang", "Serangoon", "Ang Mo Kio"],
    "East": ["Tampines", "Bedok", "Pasir Ris"],
    "West": ["Jurong West", "Jurong East", "Bukit Batok", "Bukit Panjang", "Choa Chu Kang", "Clementi"],
    "Central": ["Bishan", "Toa Payoh", "Kallang/Whampoa", "Bukit Merah", "Queenstown", "Geylang", "Marine Parade", "Central Area", "Bukit Timah"]
};
const STOREY_LOOKUP = {
    "Low": ["01 TO 03", "04 TO 06"],
    "Medium": ["07 TO 09", "10 TO 12", "13 TO 15", "16 TO 18"],
    "High": ["19 TO 21", "22 TO 24", "25 TO 27", "28 TO 30", "31 TO 33", "34 TO 36", "37 TO 39", "40 TO 42", "43 TO 45", "46 TO 48", "49 TO 51"]
};

// NEW: Data for the flat model dropdown
const FLAT_MODEL_LOOKUP = {
    "Standard/Mainstream": ["Model A", "Improved", "New Generation", "Simplified", "Apartment", "Standard", "Model A2", "Type S1", "Type S2"],
    "Premium & DBSS": ["Premium Apartment", "DBSS", "Premium Apartment Loft", "Premium Maisonette"],
    "Maisonette/Multi-gen/Adj.": ["Maisonette", "Model A-Maisonette", "Improved-Maisonette", "Adjoined flat", "Multi Generation", "3Gen"],
    "Special": ["Terrace", "2-room"]
};

// --- Event Listeners ---
document.addEventListener('DOMContentLoaded', () => {
    // Main functionality
    document.getElementById('find-btn')?.addEventListener('click', fetchRecommendations);
    document.getElementById('sort')?.addEventListener('change', sortAndRenderResults);
    document.getElementById('start-search-btn')?.addEventListener('click', () => {
        document.getElementById('search-section')?.scrollIntoView({ behavior: 'smooth' });
    });

    // Setup Dropdowns
    populateTownDropdown(); setupTownDropdownListeners(); updateTownDisplay();
    populateStoreyDropdown(); setupStoreyDropdownListeners(); updateStoreyDisplay();
    populateFlatModelDropdown(); setupFlatModelDropdownListeners(); updateFlatModelDisplay(); // NEW

    // Initial results prompt
    document.getElementById('results-container').innerHTML = '<div class="text-center py-10 px-6 bg-gray-50 rounded-lg"><i class="fas fa-search-location fa-2x text-gray-400 mb-3"></i><p class="text-gray-500">Your recommended flats will appear here.</p></div>';
});


// --- Town Dropdown Functions (Unchanged) ---
function populateTownDropdown() { /* ... unchanged ... */ }
function setupTownDropdownListeners() { /* ... unchanged ... */ }
// ... and all other town helper functions are unchanged

// --- Storey Dropdown Functions (Unchanged) ---
function populateStoreyDropdown() { /* ... unchanged ... */ }
function setupStoreyDropdownListeners() { /* ... unchanged ... */ }
// ... and all other storey helper functions are unchanged


// --- NEW: Flat Model Dropdown Functions ---
function populateFlatModelDropdown() {
    const content = document.getElementById('flat-model-dropdown-content');
    if (!content) return;
    let html = '';
    for (const category in FLAT_MODEL_LOOKUP) {
        const categoryId = category.replace(/[\s/&.]/g, '');
        html += `<div class="mb-4"><div class="flex items-center justify-between border-b pb-1 mb-2"><label class="font-semibold text-gray-800">${category}</label><div class="flex items-center cursor-pointer"><input type="checkbox" id="select-all-${categoryId}" class="flat-model-category-select-all" data-category="${categoryId}"><label for="select-all-${categoryId}" class="text-sm ml-2 cursor-pointer">Select All</label></div></div><div class="grid grid-cols-2 gap-x-4 gap-y-2">`;
        FLAT_MODEL_LOOKUP[category].forEach(model => {
            const modelId = model.replace(/\s/g, '');
            html += `<div class="flex items-center"><input type="checkbox" id="model-${modelId}" name="model" value="${model.toUpperCase()}" class="flat-model-checkbox" data-category="${categoryId}"><label for="model-${modelId}" class="ml-2 text-sm font-normal text-gray-700 cursor-pointer">${model}</label></div>`;
        });
        html += `</div></div>`;
    }
    content.innerHTML = html;
}
function setupFlatModelDropdownListeners() {
    const container = document.getElementById('flat-model-dropdown-container');
    const display = document.getElementById('flat-model-selector-display');
    const content = document.getElementById('flat-model-dropdown-content');
    display.addEventListener('click', () => content.classList.toggle('hidden'));
    document.addEventListener('click', (event) => { if (!container.contains(event.target)) content.classList.add('hidden'); });
    content.addEventListener('change', (event) => {
        const target = event.target;
        if (target.classList.contains('flat-model-checkbox')) updateFlatModelCategorySelectAllState(target.dataset.category);
        else if (target.classList.contains('flat-model-category-select-all')) handleFlatModelCategorySelectAll(target);
        updateFlatModelDisplay();
    });
}
function handleFlatModelCategorySelectAll(selectAllCheckbox) {
    const category = selectAllCheckbox.dataset.category;
    document.querySelectorAll(`.flat-model-checkbox[data-category="${category}"]`).forEach(checkbox => checkbox.checked = selectAllCheckbox.checked);
}
function updateFlatModelCategorySelectAllState(category) {
    const categoryCheckboxes = document.querySelectorAll(`.flat-model-checkbox[data-category="${category}"]`);
    document.getElementById(`select-all-${category}`).checked = Array.from(categoryCheckboxes).every(cb => cb.checked);
}
function updateFlatModelDisplay() {
    const selected = document.querySelectorAll('.flat-model-checkbox:checked');
    const selectorText = document.getElementById('flat-model-selector-text');
    selectorText.textContent = selected.length === 0 ? 'Select models...' : `${selected.length} model(s) selected`;
    selectorText.classList.toggle('text-gray-500', selected.length === 0);
}
function getSelectedFlatModels() {
    return Array.from(document.querySelectorAll('.flat-model-checkbox:checked')).map(cb => cb.value);
}


// --- Core App Logic (Modified to include flat_models) ---
async function fetchRecommendations() {
    const findButton = document.getElementById('find-btn');
    findButton.disabled = true;
    findButton.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Searching...';
    document.getElementById('results-container').innerHTML = '<div class="text-center py-10"><i class="fas fa-spinner fa-spin fa-2x text-blue-500"></i></div>';

    const constraints = {
        max_price: parseInt(document.getElementById("max-price-input").value) || 10000000,
        min_remaining_lease: parseInt(document.getElementById("min-lease-input").value) || 0,
        towns: getSelectedTowns(),
        flat_types: getSelectedPills("flat-type-pill-list").map(type => type.replace('-', ' ')),
        storey_ranges: getSelectedStoreys(),
        flat_models: getSelectedFlatModels() // MODIFIED: Using the new dropdown function
    };
    
    const priority = document.getElementById("priority-select").value;

    try {
        const response = await fetch("http://127.0.0.1:8000/recommend", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({ constraints, priority })
        });
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        currentRecommendations = data.recommendations;
        sortAndRenderResults();
    } catch (error) {
        console.error("Fetch error:", error);
        document.getElementById('results-container').innerHTML = '<div class="text-center py-10 px-6 bg-red-50 rounded-lg"><i class="fas fa-exclamation-triangle fa-2x text-red-400 mb-3"></i><p class="text-red-600">Could not retrieve recommendations. Check logs for errors.</p></div>';
    } finally {
        findButton.disabled = false;
        findButton.innerHTML = '<i class="fas fa-search mr-2"></i>Find My Home';
    }
}


// --- Other Functions (Copy/Pasting the unchanged ones for completeness) ---

// Town Dropdown Functions
function populateTownDropdown(){const content=document.getElementById('town-dropdown-content');if(!content)return;let html='';for(const region in REGION_LOOKUP){const regionId=region.replace(/\s/g,'');html+=`<div class="mb-4"><div class="flex items-center justify-between border-b pb-1 mb-2"><label class="font-semibold text-gray-800">${region}</label><div class="flex items-center cursor-pointer"><input type="checkbox" id="select-all-${regionId}" class="region-select-all" data-region="${regionId}"><label for="select-all-${regionId}" class="text-sm ml-2 cursor-pointer">Select All</label></div></div><div class="grid grid-cols-2 gap-x-4 gap-y-2">`;REGION_LOOKUP[region].forEach(town=>{const townId=town.replace(/\s|\//g,'');html+=`<div class="flex items-center"><input type="checkbox" id="town-${townId}" name="town" value="${town.toUpperCase()}" class="town-checkbox" data-region="${regionId}"><label for="town-${townId}" class="ml-2 text-sm font-normal text-gray-700 cursor-pointer">${town}</label></div>`});html+=`</div></div>`}content.innerHTML=html}
function setupTownDropdownListeners(){const container=document.getElementById('town-dropdown-container'),display=document.getElementById('town-selector-display'),content=document.getElementById('town-dropdown-content');display.addEventListener('click',()=>content.classList.toggle('hidden'));document.addEventListener('click',event=>{if(!container.contains(event.target))content.classList.add('hidden')});content.addEventListener('change',event=>{const target=event.target;if(target.classList.contains('town-checkbox'))updateRegionSelectAllState(target.dataset.region);else if(target.classList.contains('region-select-all'))handleRegionSelectAll(target);updateTownDisplay()})}
function handleRegionSelectAll(selectAllCheckbox){const region=selectAllCheckbox.dataset.region;document.querySelectorAll(`.town-checkbox[data-region="${region}"]`).forEach(checkbox=>checkbox.checked=selectAllCheckbox.checked)}
function updateRegionSelectAllState(region){const regionCheckboxes=document.querySelectorAll(`.town-checkbox[data-region="${region}"]`);document.getElementById(`select-all-${region}`).checked=Array.from(regionCheckboxes).every(cb=>cb.checked)}
function updateTownDisplay(){const selected=document.querySelectorAll('.town-checkbox:checked'),selectorText=document.getElementById('town-selector-text'),previewContainer=document.getElementById('town-preview-pills');selectorText.textContent=selected.length===0?'Select towns...':`${selected.length} town(s) selected`;selectorText.classList.toggle('text-gray-500',selected.length===0);let previewHTML='';const selectedTowns=Array.from(selected).map(cb=>cb.parentElement.querySelector('label').textContent),maxPreview=3;selectedTowns.slice(0,maxPreview).forEach(town=>{previewHTML+=`<span class="bg-gray-200 text-gray-700 text-xs font-medium px-2.5 py-1 rounded-full">${town}</span>`});if(selectedTowns.length>maxPreview)previewHTML+=`<span class="bg-gray-200 text-gray-700 text-xs font-medium px-2.5 py-1 rounded-full">+${selectedTowns.length-maxPreview} more</span>`;previewContainer.innerHTML=previewHTML}
function getSelectedTowns(){return Array.from(document.querySelectorAll('.town-checkbox:checked')).map(cb=>cb.value)}
// Storey Dropdown Functions
function populateStoreyDropdown(){const content=document.getElementById('storey-dropdown-content');if(!content)return;let html='';for(const group in STOREY_LOOKUP){const groupId=group.replace(/\s/g,'');html+=`<div class="mb-4"><div class="flex items-center justify-between border-b pb-1 mb-2"><label class="font-semibold text-gray-800">${group}</label><div class="flex items-center cursor-pointer"><input type="checkbox" id="select-all-${groupId}" class="storey-group-select-all" data-group="${groupId}"><label for="select-all-${groupId}" class="text-sm ml-2 cursor-pointer">Select All</label></div></div><div class="space-y-2">`;STOREY_LOOKUP[group].forEach(range=>{const rangeId=range.replace(/\s/g,'');html+=`<div class="flex items-center"><input type="checkbox" id="storey-${rangeId}" name="storey" value="${range}" class="storey-checkbox" data-group="${groupId}"><label for="storey-${rangeId}" class="ml-2 text-sm font-normal text-gray-700 cursor-pointer">${range}</label></div>`});html+=`</div></div>`}content.innerHTML=html}
function setupStoreyDropdownListeners(){const container=document.getElementById('storey-dropdown-container'),display=document.getElementById('storey-selector-display'),content=document.getElementById('storey-dropdown-content');display.addEventListener('click',()=>content.classList.toggle('hidden'));document.addEventListener('click',event=>{if(!container.contains(event.target))content.classList.add('hidden')});content.addEventListener('change',event=>{const target=event.target;if(target.classList.contains('storey-checkbox'))updateStoreyGroupSelectAllState(target.dataset.group);else if(target.classList.contains('storey-group-select-all'))handleStoreyGroupSelectAll(target);updateStoreyDisplay()})}
function handleStoreyGroupSelectAll(selectAllCheckbox){const group=selectAllCheckbox.dataset.group;document.querySelectorAll(`.storey-checkbox[data-group="${group}"]`).forEach(checkbox=>checkbox.checked=selectAllCheckbox.checked)}
function updateStoreyGroupSelectAllState(group){const groupCheckboxes=document.querySelectorAll(`.storey-checkbox[data-group="${group}"]`);document.getElementById(`select-all-${group}`).checked=Array.from(groupCheckboxes).every(cb=>cb.checked)}
function updateStoreyDisplay(){const selected=document.querySelectorAll('.storey-checkbox:checked'),selectorText=document.getElementById('storey-selector-text');selectorText.textContent=selected.length===0?'Select ranges...':`${selected.length} range(s) selected`;selectorText.classList.toggle('text-gray-500',selected.length===0)}
function getSelectedStoreys(){return Array.from(document.querySelectorAll('.storey-checkbox:checked')).map(cb=>cb.value)}
// Other Functions
function sortAndRenderResults(){const sortBy=document.getElementById('sort').value,sortedFlats=[...currentRecommendations];switch(sortBy){case'Price (Low to High)':sortedFlats.sort((a,b)=>a.resale_price-b.resale_price);break;case'Price (High to Low)':sortedFlats.sort((a,b)=>b.resale_price-a.resale_price);break;case'Area (Largest First)':sortedFlats.sort((a,b)=>b.floor_area_sqm-a.floor_area_sqm);break;case'Storey (Highest First)':sortedFlats.sort((a,b)=>parseInt(b.storey_range.split(' TO ')[0])-parseInt(a.storey_range.split(' TO ')[0]));break;case'Nearest MRT':sortedFlats.sort((a,b)=>(a.mrt_distance_m||Infinity)-(b.mrt_distance_m||Infinity));break;case'Recommended':default:break}renderFlats(sortedFlats)}
function getSelectedPills(containerId){const container=document.getElementById(containerId);if(!container)return[];return Array.from(container.querySelectorAll('.pill-btn.active')).map(pill=>pill.textContent.trim().replace(/\s*×$/,''))}
function togglePill(element){element.classList.toggle('active')}
function renderFlats(flats){const resultsContainer=document.getElementById('results-container'),countElement=document.querySelector('#search-section main p.text-gray-500');if(!flats||flats.length===0){resultsContainer.innerHTML='<div class="text-center py-10 px-6 bg-yellow-50 rounded-lg"><i class="fas fa-ghost fa-2x text-yellow-400 mb-3"></i><p class="text-yellow-600">No flats found matching your criteria. Try adjusting your filters.</p></div>';countElement.textContent='Found 0 matching flats.';return}countElement.textContent=`Found ${currentRecommendations.length} matching flats. Showing your top 10 best matches.`;resultsContainer.innerHTML='';flats.forEach((flat,index)=>{const cardHTML=`<div class="bg-white border border-border rounded-xl p-6 transition-all duration-300 hover:shadow-lg hover:border-cyan-300"><div class="flex justify-between items-start mb-4"><div><div class="flex items-center text-lg font-semibold text-gray-800"><span class="text-base font-bold text-gray-400 mr-2">#${index+1}</span> ${flat.street_name}, Block ${flat.block}</div><p class="text-sm text-muted-foreground ml-7">${flat.flat_type} - ${flat.flat_model}</p></div><div class="bg-[#2DD4BF] text-white text-lg font-bold px-4 py-2 rounded-full">${flat.score?flat.score.toFixed(1):'N/A'}/10</div></div><div class="grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-4 my-6"><div class="flex items-center"><div class="w-10 h-10 flex items-center justify-center bg-cyan-50 rounded-full mr-3"><i class="fas fa-dollar-sign text-[#2DD4BF] text-lg"></i></div><div><p class="text-sm text-muted-foreground">Price</p><p class="font-semibold text-gray-800">S$${flat.resale_price.toLocaleString()}</p></div></div><div class="flex items-center"><div class="w-10 h-10 flex items-center justify-center bg-cyan-50 rounded-full mr-3"><i class="fas fa-ruler-combined text-[#2DD4BF] text-lg"></i></div><div><p class="text-sm text-muted-foreground">Floor Area</p><p class="font-semibold text-gray-800">${flat.floor_area_sqm} sqm</p></div></div><div class="flex items-center"><div class="w-10 h-10 flex items-center justify-center bg-cyan-50 rounded-full mr-3"><i class="fas fa-building text-[#2DD4BF] text-lg"></i></div><div><p class="text-sm text-muted-foreground">Storey</p><p class="font-semibold text-gray-800">${flat.storey_range}</p></div></div><div class="flex items-center"><div class="w-10 h-10 flex items-center justify-center bg-cyan-50 rounded-full mr-3"><i class="fas fa-file-contract text-[#2DD4BF] text-lg"></i></div><div><p class="text-sm text-muted-foreground">Lease</p><p class="font-semibold text-gray-800">${flat.remaining_lease_years} yrs</p></div></div></div></div>`;resultsContainer.innerHTML+=cardHTML})}