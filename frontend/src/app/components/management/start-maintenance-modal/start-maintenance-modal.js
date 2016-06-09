import template from './start-maintenance-modal.html';
import ko from 'knockout';
import  { enterMaintenanceMode } from 'actions';

class StartMaintenanceModalViewModel {
    constructor({ onClose }) {
        this.onClose = onClose;
        this.hours = ko.observable(0);
        this.minutes =  ko.observable(30);
    }

    cancel() {
        this.onClose();
    }

    start() {
        enterMaintenanceMode(parseInt(this.hours()) * 60 + parseInt(this.minutes()));
        this.onClose();
    }
}

export default {
    viewModel: StartMaintenanceModalViewModel,
    template: template
};
