/*
Copyright 2019, 2020 the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package restore

import (
	"fmt"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	snapshotv1beta1api "github.com/kubernetes-csi/external-snapshotter/v2/pkg/apis/volumesnapshot/v1beta1"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// PVCRestoreItemAction is a restore item action plugin for Velero
type PVCRestoreItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating that the PVCRestoreItemAction should be run while restoring PVCs.
func (p *PVCRestoreItemAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumeclaims"},
		//TODO: add label selector volumeSnapshotLabel
	}, nil
}

func resetPVCAnnotations(pvc *corev1api.PersistentVolumeClaim, preserve []string) {
	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
		return
	}
	for k := range pvc.Annotations {
		if !util.Contains(preserve, k) {
			delete(pvc.Annotations, k)
		}
	}
}

func resetPVCSpec(pvc *corev1api.PersistentVolumeClaim, vsName string) {
	// Restore operation for the PVC will use the volumesnapshot as the data source.
	// So clear out the volume name, which is a ref to the PV
	pvc.Spec.VolumeName = ""
	pvc.Spec.DataSource = &corev1api.TypedLocalObjectReference{
		APIGroup: &snapshotv1beta1api.SchemeGroupVersion.Group,
		Kind:     "VolumeSnapshot",
		Name:     vsName,
	}
}

func setPVCStorageResourceRequest(pvc *corev1api.PersistentVolumeClaim, restoreSize resource.Quantity, log logrus.FieldLogger) {
	{
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1api.ResourceList{}
		}

		storageReq, exists := pvc.Spec.Resources.Requests[corev1api.ResourceStorage]
		if !exists || storageReq.Cmp(restoreSize) < 0 {
			pvc.Spec.Resources.Requests[corev1api.ResourceStorage] = restoreSize
			rs := pvc.Spec.Resources.Requests[corev1api.ResourceStorage]
			log.Infof("Resetting storage requests for PVC %s/%s to %s", pvc.Namespace, pvc.Name, rs.String())
		}
	}
}

// Execute modifies the PVC's spec to use the volumesnapshot object as the data source ensuring that the newly provisioned volume
// can be pre-populated with data from the volumesnapshot.
func (p *PVCRestoreItemAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	var pvc corev1api.PersistentVolumeClaim
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.Item.UnstructuredContent(), &pvc); err != nil {
		return nil, errors.WithStack(err)
	}
	p.Log.Infof("Starting PVCRestoreItemAction for PVC %s/%s", pvc.Namespace, pvc.Name)

	resetPVCAnnotations(&pvc, []string{velerov1api.BackupNameLabel, util.VolumeSnapshotLabel})

	volumeSnapshotName, ok := pvc.Annotations[util.VolumeSnapshotLabel]
	if !ok {
		p.Log.Infof("Skipping PVCRestoreItemAction for PVC %s/%s, PVC does not have a CSI volumesnapshot.", pvc.Namespace, pvc.Name)
		return &velero.RestoreItemActionExecuteOutput{
			UpdatedItem: input.Item,
		}, nil
	}

	kubeClient, snapClient, err := util.GetClients()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	vs, err := snapClient.SnapshotV1beta1().VolumeSnapshots(pvc.Namespace).Get(volumeSnapshotName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Sprintf("Failed to get Volumesnapshot %s/%s to restore PVC %s/%s", pvc.Namespace, volumeSnapshotName, pvc.Namespace, pvc.Name))
	}

	if _, exists := vs.Annotations[util.VolumeSnapshotRestoreSize]; exists {
		restoreSize, err := resource.ParseQuantity(vs.Annotations[util.VolumeSnapshotRestoreSize])
		if err != nil {
			return nil, errors.Wrapf(err, fmt.Sprintf("Failed to parse %s from annotation on Volumesnapshot %s/%s into restore size",
				vs.Annotations[util.VolumeSnapshotRestoreSize], vs.Namespace, vs.Name))
		}
		// It is possible that the volume provider allocated a larger capacity volume than what was requested in the backed up PVC.
		// In this scenario the volumesnapshot of the PVC will endup being larger than its requested storage size.
		// Such a PVC, on restore as-is, will be stuck attempting to use a Volumesnapshot as a data source for a PVC that
		// is not large enough.
		// To counter that, here we set the storage request on the PVC to the larger of the PVC's storage request and the size of the
		// VolumeSnapshot
		setPVCStorageResourceRequest(&pvc, restoreSize, p.Log)
	}

	resetPVCSpec(&pvc, volumeSnapshotName)

	p.Log.Info("Ready to hack PVC")
	if err = createPVC(&pvc, kubeClient.CoreV1(), p.Log); err != nil {
		p.Log.Errorf("Failed to create PVC: %v", err)
		return nil, errors.WithStack(err)
	}
	p.Log.Infof("PVC %s created", pvc.Name)

	p.Log.Infof("Returning from PVCRestoreItemAction for PVC %s/%s", pvc.Namespace, pvc.Name)

	return &velero.RestoreItemActionExecuteOutput{
		SkipRestore: true,
	}, nil
}

func createPVC(pvc *corev1api.PersistentVolumeClaim, corev1interface corev1.CoreV1Interface, logger logrus.FieldLogger) error {
	logger.Infof("Original PVC:  %v", pvc)
	pvc, err := handleUnstructuredPVC(pvc)
	if err != nil {
		return errors.Wrap(err, "Failed to handle unstructured PVC for the purpose of dynamic provisioning")
	}

	logger.Infof("PVC constructed:  %v", pvc)
	createdPVC, err := corev1interface.PersistentVolumeClaims(pvc.Namespace).Create(pvc)
	if err != nil {
		return errors.Wrap(err, "Failed to create PVC")
	}
	logger.Infof("PVC created: %v", createdPVC)
	return nil
}

func handleUnstructuredPVC(obj runtime.Object) (*corev1api.PersistentVolumeClaim, error) {
	unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&obj)
	if err != nil {
		return nil, err
	}
	unstructuredPVC := &unstructured.Unstructured{Object: unstructuredObj}

	// handle PVC for the purpose of dynamic provisioning
	pvc, ok := obj.(*corev1api.PersistentVolumeClaim)
	if ok {
		if pvc.Spec.VolumeName != ""{
			// use the unstructured helpers here since we're only deleting and
			// the unstructured converter will add back (empty) fields for metadata
			// and status that we removed earlier.
			unstructured.RemoveNestedField(unstructuredPVC.Object, "spec", "volumeName")
			annotations := unstructuredPVC.GetAnnotations()
			delete(annotations, "pv.kubernetes.io/bind-completed")
			delete(annotations, "pv.kubernetes.io/bound-by-controller")
			unstructuredPVC.SetAnnotations(annotations)
		}
	}

	updatedPVC := new(corev1api.PersistentVolumeClaim)
	if err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPVC.Object, updatedPVC); err != nil {
		return nil, errors.Wrap(err, "Failed to convert object from unstructured to PVC")
	}

	return updatedPVC, nil
}

func appendUnstructuredPVC(list *unstructured.UnstructuredList, obj runtime.Object) error {
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&obj)

	// Remove the status field so we're not sending blank data to the server.
	// On CRDs, having an empty status is actually a validation error.
	delete(u, "status")
	if err != nil {
		return err
	}
	list.Items = append(list.Items, unstructured.Unstructured{Object: u})
	return nil
}
